from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from influxdb_client.client.influxdb_client import InfluxDBClient as Client
from influxdb_client.client.write.point import Point
from influxdb_client.client.write_api import SYNCHRONOUS

from .models import DAQEvent


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
            self.write_api.write(
                bucket=self.bucket,
                org=self.org,
                record="daq value=1i",
            )
            self.logger.info("[InfluxDB] Write succeeded")
        except Exception as exc:
            self.logger.error("[InfluxDB] Write failed error=%s", str(exc))
            raise

    def _measurement_for_producer(self, producer_id: str) -> str:
        safe = re.sub(r"[^A-Za-z0-9_-]", "_", producer_id)
        safe = safe or "unknown"
        return f"{self.measurement_prefix}_{safe}"

    def _parse_timestamp(self, value: str) -> datetime | None:
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None