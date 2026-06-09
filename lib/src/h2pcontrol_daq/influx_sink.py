from __future__ import annotations

import math
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .models import DAQConfig, DAQEvent

_SKIPPED_FIELD_KEYS = {
    "analysis",
    "metadata",
    "tags",
    "board_info",
    "observed_at",
    "port",
    "version",
    "zero_offset",
}

_BASE_TAG_KEYS = {"run_id", "producer_id", "source", "method", "direction"}


@dataclass(frozen=True, slots=True)
class LocalInfluxConfig:
    url: str
    token: str
    org: str
    bucket: str
    measurement_prefix: str = "daq"

    @classmethod
    def from_daq_config(cls, config: DAQConfig) -> "LocalInfluxConfig":
        return cls(
            url=config.influxdb_url or os.getenv("INFLUXDB_URL", "http://localhost:8086"),
            token=(
                config.influxdb_token
                or os.getenv("INFLUXDB_TOKEN")
                or os.getenv("INFLUXDB_ADMIN_TOKEN")
                or ""
            ),
            org=config.influxdb_org or os.getenv("INFLUXDB_ORG", "beyer-labs"),
            bucket=config.influxdb_bucket or os.getenv("INFLUXDB_BUCKET", "h2pcontrol"),
            measurement_prefix=(
                config.influxdb_measurement_prefix
                or os.getenv("INFLUXDB_MEASUREMENT_PREFIX", "daq")
            ),
        )


class LocalInfluxSink:
    def __init__(self, config: LocalInfluxConfig) -> None:
        try:
            from influxdb_client import InfluxDBClient
            from influxdb_client.client.write_api import SYNCHRONOUS
        except ImportError as exc:
            raise RuntimeError(
                "Local InfluxDB writes require the 'influxdb-client' package. "
                "Install h2pcontrol-daq with its current dependencies, then set "
                "DAQConfig(enable_local_influx=True)."
            ) from exc

        if not config.token:
            raise ValueError(
                "Local InfluxDB writes are enabled but no token was provided. "
                "Set DAQConfig.influxdb_token, INFLUXDB_TOKEN, or INFLUXDB_ADMIN_TOKEN."
            )

        self.config = config
        self._client = InfluxDBClient(
            url=config.url,
            token=config.token,
            org=config.org,
        )
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)

    @classmethod
    def from_daq_config(cls, config: DAQConfig) -> "LocalInfluxSink":
        return cls(LocalInfluxConfig.from_daq_config(config))

    def write_event(self, event: DAQEvent) -> None:
        point = event_to_point(event, self.config.measurement_prefix)
        self._write_api.write(
            bucket=self.config.bucket,
            org=self.config.org,
            record=point,
        )

    def close(self) -> None:
        self._client.close()


def event_to_point(event: DAQEvent, measurement_prefix: str = "daq"):
    from influxdb_client import Point

    point = Point(_measurement_name(measurement_prefix, event.source))
    point.tag("run_id", str(event.run_id))
    point.tag("producer_id", str(event.producer_id))
    point.tag("source", str(event.source))
    point.tag("method", str(event.method))
    point.tag("direction", str(event.direction))
    _add_event_tags(point, event.tags)

    timestamp = _parse_timestamp(event.timestamp)
    if timestamp is not None:
        point.time(timestamp)

    fields = _flatten_measurement_fields(event.data, prefix="data")
    if not fields:
        fields["event_count"] = 1

    for key, value in fields.items():
        point.field(key, value)

    return point


def _add_event_tags(point: Any, tags: Mapping[str, Any]) -> None:
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
    cleaned_prefix = _safe_influx_key(prefix or "daq")
    cleaned_source = _safe_influx_key(source or "unknown")
    return f"{cleaned_prefix}_{cleaned_source}"


def _flatten_measurement_fields(value: Any, *, prefix: str) -> dict[str, bool | int | float]:
    if isinstance(value, Mapping):
        flattened: dict[str, bool | int | float] = {}
        for key, child in value.items():
            if _skip_field_key(key):
                continue
            child_prefix = _safe_influx_key(f"{prefix}_{key}")
            flattened.update(_flatten_measurement_fields(child, prefix=child_prefix))
        return flattened

    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        flattened = {}
        for index, child in enumerate(value):
            flattened.update(_flatten_measurement_fields(child, prefix=f"{prefix}_{index}"))
        return flattened

    scalar = _measurement_scalar(value)
    if scalar is None:
        return {}
    return {_safe_influx_key(prefix): scalar}


def _measurement_scalar(value: Any) -> bool | int | float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return None


def _skip_field_key(value: Any) -> bool:
    return _safe_influx_key(value).lower() in _SKIPPED_FIELD_KEYS


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
