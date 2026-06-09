from __future__ import annotations

import logging
import math
import os
import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from influxdb_client.client.influxdb_client import InfluxDBClient as Client
from influxdb_client.client.write.point import Point
from influxdb_client.client.write_api import SYNCHRONOUS

from .models import DAQEvent

_BASE_TAG_KEYS = {"run_id", "producer_id", "source", "method", "direction"}


class InfluxDBConfig:
    def __init__(self, env_path: Path | None = None) -> None:
        loaded = False
        env_candidates: list[Path] = []
        if env_path is not None:
            env_candidates.append(env_path)
        env_candidates.extend(
            [
                Path(__file__).with_name(".env"),
                Path(__file__).resolve().parents[1] / ".env",
            ]
        )

        for candidate in env_candidates:
            if candidate.is_file():
                load_dotenv(dotenv_path=candidate, override=False)
                loaded = True
                break

        if not loaded:
            load_dotenv()
        self.url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
        self.token = os.getenv("INFLUXDB_ADMIN_TOKEN", "")
        self.org = os.getenv("INFLUXDB_ORG", "beyer-labs")
        self.bucket = os.getenv("INFLUXDB_BUCKET", "h2pcontrol")
        self.measurement_prefix = os.getenv("INFLUXDB_MEASUREMENT_PREFIX", "producer")


class InfluxDBClient:
    def __init__(self, config: InfluxDBConfig, logger: logging.Logger | None = None) -> None:
        self.client = Client(url=config.url, token=config.token, org=config.org)
        self.bucket = config.bucket
        self.org = config.org
        self.measurement_prefix = config.measurement_prefix
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.logger = logger or logging.getLogger(__name__)

        self.logger.info(
            "[InfluxDB] Config org=%s bucket=%s prefix=%s token_set=%s",
            self.org,
            self.bucket,
            self.measurement_prefix,
            bool(config.token),
        )

        if not config.token:
            self.logger.warning("[InfluxDB] INFLUXDB_ADMIN_TOKEN is empty; writes will fail.")
        if not config.bucket:
            self.logger.warning("[InfluxDB] INFLUXDB_BUCKET is empty; writes will fail.")

    def close(self) -> None:
        try:
            self.write_api.flush()
        finally:
            self.client.close()

    def write_event(self, event: DAQEvent) -> None:
        try:
            point = event_to_point(event, self.measurement_prefix)
            self.write_api.write(
                bucket=self.bucket,
                org=self.org,
                record=point,
            )
            self.logger.info("[InfluxDB] Write succeeded event_id=%s", event.event_id)
        except Exception as exc:
            self.logger.error("[InfluxDB] Write failed error=%s", str(exc))
            raise


def event_to_point(event: DAQEvent, measurement_prefix: str = "producer") -> Point:
    point = Point(_measurement_name(measurement_prefix, event.source))
    point.tag("run_id", str(event.run_id))
    point.tag("producer_id", str(event.producer_id))
    point.tag("source", str(event.source))
    point.tag("method", str(event.method))
    point.tag("direction", str(event.direction))
    _add_event_tags(point, event.tags)
    point.field("event_id", int(event.event_id))

    timestamp = _parse_timestamp(event.timestamp)
    if timestamp is not None:
        point.time(timestamp)

    fields = _flatten_fields(event.data, prefix="data")
    if not fields:
        fields["event_count"] = 1

    for key, value in fields.items():
        point.field(key, value)

    return point


def _add_event_tags(point: Point, tags: Mapping[str, Any]) -> None:
    for key, value in _flatten_tags(tags).items():
        tag_key = _safe_influx_key(key)
        if tag_key in _BASE_TAG_KEYS:
            tag_key = f"event_{tag_key}"
        point.tag(tag_key, str(value))


def _flatten_tags(value: Any, *, prefix: str = "") -> dict[str, str | bool | int | float]:
    if isinstance(value, Mapping):
        flattened: dict[str, str | bool | int | float] = {}
        for key, child in value.items():
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            flattened.update(_flatten_tags(child, prefix=child_prefix))
        return flattened

    scalar = _tag_scalar(value)
    if scalar is None or not prefix:
        return {}
    return {prefix: scalar}


def _tag_scalar(value: Any) -> str | bool | int | float | None:
    if value is None:
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, bool | int | float | str):
        return value
    return str(value)


def _measurement_name(prefix: str, source: str) -> str:
    cleaned_prefix = _safe_influx_key(prefix or "producer")
    cleaned_source = _safe_influx_key(source or "unknown")
    return f"{cleaned_prefix}_{cleaned_source}"


def _flatten_fields(value: Any, *, prefix: str) -> dict[str, bool | int | float | str]:
    if isinstance(value, Mapping):
        flattened: dict[str, bool | int | float | str] = {}
        for key, child in value.items():
            child_prefix = _safe_influx_key(f"{prefix}_{key}")
            flattened.update(_flatten_fields(child, prefix=child_prefix))
        return flattened

    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        flattened = {}
        for index, child in enumerate(value):
            flattened.update(_flatten_fields(child, prefix=f"{prefix}_{index}"))
        return flattened

    scalar = _influx_scalar(value)
    if scalar is None:
        return {}
    return {_safe_influx_key(prefix): scalar}


def _influx_scalar(value: Any) -> bool | int | float | str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, sort_keys=True)
    except TypeError:
        return str(value)


def _safe_influx_key(value: Any) -> str:
    text = str(value).strip()
    text = re.sub(r"[^0-9A-Za-z_]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "value"


def _parse_timestamp(value: Any) -> datetime | None:
    text = str(value).strip()
    if not text:
        return None

    try:
        number = float(text)
    except ValueError:
        number = math.nan

    if math.isfinite(number):
        seconds = number / 1000.0 if abs(number) >= 1_000_000_000_000 else number
        return datetime.fromtimestamp(seconds, tz=timezone.utc)

    iso_text = text.replace("Z", "+00:00")
    try:
        timestamp = datetime.fromisoformat(iso_text)
    except ValueError:
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp
