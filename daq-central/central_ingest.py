"""
Minimal central ingest service for h2pcontrol-daq.

This module subscribes to the existing DAQPipeline UI broadcaster and writes
each received event to:

- a CSV event log (partitioned by source)
- a JSON metadata sidecar

The implementation is intentionally small and single-purpose so it can serve as
the first central aggregation step before the rest of the server is built out.
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from central_connector_pipeline import DAQEvent, DAQPipeline


LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class CentralIngestConfig:
    """Runtime configuration for the minimal central ingest service."""

    run_id: str = "central_run"
    output_dir: Path = field(default_factory=lambda: Path("./daq-central-data"))
    queue_size: int = 10_000
    partition_by_source: bool = True


class MinimalCentralIngest:
    """Subscribe to the central relay and persist incoming events."""

    def __init__(self, source_pipeline: DAQPipeline, config: CentralIngestConfig | None = None):
        self.source_pipeline = source_pipeline
        self.config = config or CentralIngestConfig()

        self.base_dir = self.config.output_dir / self.config.run_id
        self.csv_path = self.base_dir / "events.csv"
        self.metadata_path = self.base_dir / "metadata.json"

        self._queue: asyncio.Queue[DAQEvent] = asyncio.Queue(maxsize=self.config.queue_size)
        self._task: asyncio.Task | None = None
        self._stopping = False

        self._event_count = 0
        self._first_timestamp_ns: int | None = None
        self._last_timestamp_ns: int | None = None
        self._seen_sources: set[str] = set()
        self._seen_methods: set[str] = set()
        self._last_error: str | None = None
        self._events_by_source: dict[str, int] = {}


    @property
    def queue(self) -> asyncio.Queue[DAQEvent]:
        return self._queue

    async def start(self) -> None:
        """Register with the central relay and start consuming events."""
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.source_pipeline.subscribe(self._queue)
        self._task = asyncio.create_task(self._consume_events(), name="central-ingest")

    async def stop(self) -> None:
        """Stop consuming events and close any open client resources."""
        self._stopping = True
        self.source_pipeline.unsubscribe(self._queue)

        await self._queue.join()

        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        self._write_metadata()

    async def _consume_events(self) -> None:
        while True:
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=0.2)
            except asyncio.TimeoutError:
                if self._stopping and self._queue.empty():
                    break
                continue

            try:
                await asyncio.to_thread(self._persist_event, event)
            except Exception as exc:  # pragma: no cover - defensive guard
                self._last_error = f"{type(exc).__name__}: {exc}"
                LOGGER.exception("Failed to persist central ingest event")
            finally:
                self._queue.task_done()

            if self._stopping and self._queue.empty():
                break

    def _persist_event(self, event: DAQEvent) -> None:
        self._event_count += 1
        self._first_timestamp_ns = self._first_timestamp_ns or event.timestamp_ns
        self._last_timestamp_ns = event.timestamp_ns
        self._seen_sources.add(event.source)
        self._seen_methods.add(f"{event.source}.{event.method}")
        self._events_by_source[event.source] = self._events_by_source.get(event.source, 0) + 1

        self._append_csv_row(event)
        self._write_metadata()

    def _append_csv_row(self, event: DAQEvent) -> None:
        row = {
            "timestamp_ns": event.timestamp_ns,
            "run_id": event.run_id,
            "source": event.source,
            "method": event.method,
            "direction": event.direction,
            "sequence": event.sequence,
            "quality": event.quality,
            "payload_json": json.dumps(event.payload, sort_keys=True),
            "units_json": json.dumps(event.units, sort_keys=True),
        }

        fieldnames = list(row.keys())
        csv_path = self._csv_path_for_event(event)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        file_exists = csv_path.exists()
        with csv_path.open("a", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)


    def _write_metadata(self) -> None:
        metadata = {
            "run_id": self.config.run_id,
            "source_pipeline_run_id": self.source_pipeline.run_id,
            "output_dir": str(self.base_dir),
            "event_count": self._event_count,
            "events_by_source": dict(sorted(self._events_by_source.items())),
            "first_timestamp_ns": self._first_timestamp_ns,
            "last_timestamp_ns": self._last_timestamp_ns,
            "seen_sources": sorted(self._seen_sources),
            "seen_methods": sorted(self._seen_methods),
            "last_error": self._last_error,
            "created_at_ns": self._first_timestamp_ns or time.time_ns(),
        }

        self.metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")

    @staticmethod
    def _safe_source_name(source: str) -> str:
        cleaned = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in source)
        return cleaned or "unknown"

    def _csv_path_for_event(self, event: DAQEvent) -> Path:
        if not self.config.partition_by_source:
            return self.csv_path
        source_dir = self.base_dir / self._safe_source_name(event.source)
        return source_dir / "events.csv"

    


__all__ = ["CentralIngestConfig", "MinimalCentralIngest"]
