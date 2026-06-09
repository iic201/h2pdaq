from __future__ import annotations
import asyncio
import contextlib
import time
import json
import h5py
import logging
import csv
import os
import socket
import uuid
from collections.abc import Mapping, Sequence
from typing import Any, Literal, cast
from .models import DAQEvent, PendingEvent, OverflowPolicy, DAQConfig, LocalDAQStats
from .centralDAQ_connector.grpc_central_sink import GrpcDAQSink
from .influx_sink import LocalInfluxSink
from pathlib import Path
from .buffer.preview import PreviewFrame

_PROCESS_START_NS = time.time_ns()
_RAW_RUN_ID = f"{socket.gethostname()}_{os.getpid()}_{_PROCESS_START_NS}"
_RUN_ID = uuid.uuid5(uuid.NAMESPACE_DNS, _RAW_RUN_ID).hex


def get_run_id() -> str:
    return str(_RUN_ID)

class LocalDAQ:
    def __init__(self, config: DAQConfig | None = None) -> None:
        self.config = config if config is not None else DAQConfig()
        self.stats = LocalDAQStats()
        self.logger = self.setup_logger()

        self._ingress_q: asyncio.Queue[PendingEvent] = asyncio.Queue(
            maxsize=self.config.ingress_maxsize
        )
        # Uncomment if you want to output JSONL
        # self._outbound_jsonl_q: asyncio.Queue[DAQEvent] = asyncio.Queue(
        #     maxsize=self.config.outbound_maxsize
        # )
        self._outbound_hdf5_q: asyncio.Queue[DAQEvent] = asyncio.Queue(
            maxsize=self.config.outbound_maxsize
        )
        self._outbound_csv_q: asyncio.Queue[DAQEvent] = asyncio.Queue(
            maxsize=self.config.outbound_maxsize
        )
        self._outbound_central_q: asyncio.Queue[DAQEvent] = asyncio.Queue(
            maxsize=self.config.outbound_maxsize
        )
        self._outbound_influx_q: asyncio.Queue[DAQEvent] = asyncio.Queue(
            maxsize=self.config.outbound_maxsize
        )
        self._tasks: list[asyncio.Task] = []
        self._stopping = False
        self._data_path = self._create_data_path()
        self._event_id_counter = 1

        self.central_sink: GrpcDAQSink = GrpcDAQSink(
            central_address=self.config.central_daq_address or "127.0.0.1:50052",
            queue=self._outbound_central_q,
            logger=self.logger
        )
        self.local_influx_sink: LocalInfluxSink | None = None

    async def start(self) -> None:
        self.logger.info("Starting DAQ with config: %s", self.config)
        if self.config.enable_local_influx:
            self.local_influx_sink = LocalInfluxSink.from_daq_config(self.config)
            self.logger.info("[I] Local InfluxDB writes enabled.")

        # Create one worker for the serializer and one each for the writers.
        self._tasks = [
            asyncio.create_task(self._serializer_loop(),
                                name="daq-serializer"),
            # Uncomment if you want to output JSONL
            # asyncio.create_task(self._writer_jsonl_loop(),
            #                     name="daq-writer-jsonl"),
            asyncio.create_task(self._writer_hdf5_loop(),
                                name="daq-writer-hdf5"),
            asyncio.create_task(self._writer_csv_loop(),
                                name="daq-writer-csv"),
        ]

        if self.config.enable_central_stream:
            self.logger.info(
                "[C] Central stream enabled. Starting central sink task.")
            self._tasks.append(
                asyncio.create_task(self.central_sink.run(),
                                    name="daq-central-sink")
            )

        if self.config.enable_local_influx:
            self._tasks.append(
                asyncio.create_task(self._writer_influx_loop(),
                                    name="daq-writer-influx")
            )

    async def stop(self) -> None:
        dropped_outbound = (
            # Uncomment if you want to output JSONL
            # self.stats.dropped_outbound_jsonl
            + self.stats.dropped_outbound_hdf5
            + self.stats.dropped_outbound_csv
            + self.stats.dropped_outbound_central
            + self.stats.dropped_outbound_influx
        )
        self.logger.info("Stopping DAQ having published %s events. Having dropped %s ingress events and %s outbound events.\n",
                         self.stats.published, self.stats.dropped_ingress, dropped_outbound)
        self._stopping = True
        # Wait for queues to be fully processed before cancelling tasks.
        await self._ingress_q.join()
        # Uncomment if you want to output JSONL
        # await self._outbound_jsonl_q.join()
        await self._outbound_hdf5_q.join()
        await self._outbound_csv_q.join()
        if self.config.enable_central_stream:
            await self._join_or_drop_queue(
                self._outbound_central_q,
                timeout=self.config.central_flush_timeout_s,
                dropped_counter="dropped_outbound_central",
                queue_name="central stream",
            )
        await self._outbound_influx_q.join()
        await self.central_sink.stop()
        # Cancel any remaining tasks
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self.local_influx_sink is not None:
            self.local_influx_sink.close()

    def _create_data_path(self) -> bool:
        Path("data").mkdir(parents=True, exist_ok=True)
        if Path("data").exists():
            return True
        else:
            self.logger.error(
                "Failed to create data directory at path: %s", Path("data"))
            return False

    def publish_pending_event(self, event: PendingEvent) -> None:
        event.event_id = self._event_id_counter
        self._event_id_counter += 1
        self.stats.published += 1
        self._queue_put_now(self._ingress_q, event,
                            self.config.ingress_overflow, "ingress")

    def _queue_put_now(self, q: asyncio.Queue, item, policy: OverflowPolicy, queue_name: str,) -> None:
        if not q.full():
            q.put_nowait(item)
            return

        if queue_name == "ingress":
            counter_name = "dropped_ingress"
        elif queue_name == "outbound_jsonl":
            counter_name = "dropped_outbound_jsonl"
        elif queue_name == "outbound_hdf5":
            counter_name = "dropped_outbound_hdf5"
        elif queue_name == "outbound_csv":
            counter_name = "dropped_outbound_csv"
        elif queue_name == "outbound_central":
            counter_name = "dropped_outbound_central"
        elif queue_name == "outbound_influx":
            counter_name = "dropped_outbound_influx"
        else:
            raise ValueError(f"Unknown queue name: {queue_name}")

        if policy == OverflowPolicy.DROP_NEWEST:
            setattr(self.stats, counter_name, getattr(
                self.stats, counter_name) + 1)
            return

        if policy == OverflowPolicy.DROP_OLDEST:
            with contextlib.suppress(asyncio.QueueEmpty):
                q.get_nowait()
                q.task_done()
                setattr(self.stats, counter_name, getattr(
                    self.stats, counter_name) + 1)
            q.put_nowait(item)
            return

        # BLOCK_WITH_TIMEOUT: schedule async put without blocking the caller too long.
        asyncio.create_task(self._put_with_timeout(q, item, counter_name))

    async def _put_with_timeout(self, q: asyncio.Queue, item, counter_name: str) -> None:
        try:
            await asyncio.wait_for(q.put(item), timeout=self.config.queue_put_timeout_s)
        except asyncio.TimeoutError:
            setattr(self.stats, counter_name, getattr(
                self.stats, counter_name) + 1)

    async def _join_or_drop_queue(
        self,
        q: asyncio.Queue,
        *,
        timeout: float,
        dropped_counter: str,
        queue_name: str,
    ) -> None:
        try:
            await asyncio.wait_for(q.join(), timeout=max(0.0, timeout))
            return
        except asyncio.TimeoutError:
            pass

        dropped = self._drop_queued_items(q, dropped_counter)
        self.logger.warning(
            "Timed out after %.2fs flushing %s; dropped %d queued events.",
            timeout,
            queue_name,
            dropped,
        )

    def _drop_queued_items(self, q: asyncio.Queue, dropped_counter: str) -> int:
        dropped = 0
        while True:
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                break
            q.task_done()
            dropped += 1

        setattr(self.stats, dropped_counter, getattr(self.stats, dropped_counter) + dropped)
        return dropped

    async def _serializer_loop(self) -> None:
        while True:
            pending = await self._get_or_stop_pending(self._ingress_q, timeout=0.2)
            if pending is None:
                if self._stopping:
                    break
                continue

            try:
                tags = _normalize_tags(
                    {
                        **_normalize_tags(_extract_tags_from_data(pending.data)),
                        **_normalize_tags(pending.tags if isinstance(pending.tags, Mapping) else {}),
                    }
                )
                event = DAQEvent(
                    event_id=pending.event_id,
                    run_id=pending.run_id,
                    timestamp=pending.timestamp,
                    producer_id=pending.producer_id,
                    source=pending.source,
                    method=pending.method,
                    direction=pending.direction,
                    data=pending.data,
                    tags=tags,
                )
                self.stats.serialized += 1
                # self._queue_put_now(
                #     self._outbound_jsonl_q,
                #     event,
                #     self.config.outbound_overflow,
                #     "outbound_jsonl",
                # )
                self._queue_put_now(
                    self._outbound_csv_q,
                    event,
                    self.config.outbound_overflow,
                    "outbound_csv",
                )
                self._queue_put_now(
                    self._outbound_hdf5_q,
                    event,
                    self.config.outbound_overflow,
                    "outbound_hdf5",
                )

                if self.config.enable_central_stream:
                    self.logger.info(
                        "[C] Central stream enabled. Queuing event.")
                    self._queue_put_now(
                        self._outbound_central_q,
                        event,
                        self.config.outbound_overflow,
                        "outbound_central",
                    )

                if self.config.enable_local_influx:
                    self._queue_put_now(
                        self._outbound_influx_q,
                        event,
                        self.config.outbound_overflow,
                        "outbound_influx",
                    )
            except Exception:
                self.stats.serialization_errors += 1
            finally:
                self._ingress_q.task_done()

    async def _writer_jsonl_loop(self) -> None:
        batch: list[DAQEvent] = []
        last_flush = time.monotonic()

        while True:
            event = await self._get_or_stop_daq_event(self._outbound_jsonl_q, timeout=0.5)
            if event is not None:
                batch.append(event)

            now = time.monotonic()
            flush_due = batch and (now - last_flush >= 1.0)
            flush_on_stop = batch and self._stopping and self._outbound_jsonl_q.empty()

            if flush_due or flush_on_stop:
                try:
                    await self._write_jsonl(batch)
                except Exception:
                    self.logger.exception(
                        "Failed to write batch of %d events", len(batch))
                finally:
                    for _ in batch:
                        self._outbound_jsonl_q.task_done()
                    batch.clear()
                    last_flush = time.monotonic()

            if self._stopping and self._outbound_jsonl_q.empty() and not batch:
                break

    async def _writer_csv_loop(self) -> None:
        batch: list[DAQEvent] = []
        last_flush = time.monotonic()

        while True:
            event = await self._get_or_stop_daq_event(self._outbound_csv_q, timeout=0.5)
            if event is not None:
                batch.append(event)

            now = time.monotonic()
            flush_due = batch and (now - last_flush >= 1.0)
            flush_on_stop = batch and self._stopping and self._outbound_csv_q.empty()

            if flush_due or flush_on_stop:
                try:
                    await self._write_csv(batch)
                except Exception:
                    self.logger.exception(
                        "Failed to write batch of %d events", len(batch))
                finally:
                    for _ in batch:
                        self._outbound_csv_q.task_done()
                    batch.clear()
                    last_flush = time.monotonic()

            if self._stopping and self._outbound_csv_q.empty() and not batch:
                break

    async def _writer_hdf5_loop(self) -> None:
        batch: list[DAQEvent] = []
        last_flush = time.monotonic()

        while True:
            event = await self._get_or_stop_daq_event(self._outbound_hdf5_q, timeout=0.5)
            if event is not None:
                batch.append(event)

            now = time.monotonic()
            flush_due = batch and (now - last_flush >= 1.0)
            flush_on_stop = batch and self._stopping and self._outbound_hdf5_q.empty()

            if flush_due or flush_on_stop:
                try:
                    await self._write_hdf5(batch)
                except Exception:
                    self.logger.exception(
                        "Failed to write batch of %d events", len(batch))
                finally:
                    for _ in batch:
                        self._outbound_hdf5_q.task_done()
                    batch.clear()
                    last_flush = time.monotonic()

            if self._stopping and self._outbound_hdf5_q.empty() and not batch:
                break

    async def _writer_influx_loop(self) -> None:
        while True:
            event = await self._get_or_stop_daq_event(self._outbound_influx_q, timeout=0.5)
            if event is not None:
                try:
                    if self.local_influx_sink is None:
                        raise RuntimeError("Local InfluxDB sink was not initialized")
                    await asyncio.to_thread(self.local_influx_sink.write_event, event)
                except Exception:
                    self.stats.dropped_outbound_influx += 1
                    self.logger.exception("Failed to write event %s to InfluxDB", event.event_id)
                finally:
                    self._outbound_influx_q.task_done()

            if self._stopping and self._outbound_influx_q.empty():
                break

    async def _get_or_stop_pending(self, queue: asyncio.Queue, timeout: float) -> PendingEvent | None:
        # self.logger.info("Attempting to get item from queue (%s) with timeout: %f", queue, timeout)
        try:
            return await asyncio.wait_for(queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    async def _get_or_stop_daq_event(self, queue: asyncio.Queue, timeout: float) -> DAQEvent | None:
        # self.logger.info("Attempting to get item from queue (%s) with timeout: %f", queue, timeout)
        try:
            return await asyncio.wait_for(queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    def setup_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        logger.propagate = False

        for handler in list(logger.handlers):
            logger.removeHandler(handler)

        Path(".logs").mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(
            ".logs/daq-info.log", encoding="utf-8")
        error_file_handler = logging.FileHandler(
            ".logs/daq-error.log", encoding="utf-8")
        error_file_handler.setLevel(logging.ERROR)
        file_handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        error_file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(error_file_handler)

        return logger

    async def _write_jsonl(self, events: list[DAQEvent]) -> None:
        if not self._data_path:
            self.logger.error(
                "[!] Data path not available. Cannot write JSONL.")
            return

        Path("data/jsonl").mkdir(parents=True, exist_ok=True)

        for event in events:
            path = Path("data/jsonl") / \
                f"daq_capture_{_safe_filename_part(event.run_id)}.jsonl"
            record = {
                "event_id": event.event_id,
                "timestamp": event.timestamp,
                "run_id": event.run_id,
                "producer_id": event.producer_id,
                "source": event.source,
                "method": event.method,
                "direction": event.direction,
                "tags": event.tags,
                "message": event.data,
            }
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")

        self.logger.info("Flushed %d events of type jsonl", len(events))

    async def _write_csv(self, events: list[DAQEvent]) -> None:
        if not self._data_path:
            self.logger.error("[!] Data path not available. Cannot write CSV.")
            return

        Path("data/csv").mkdir(parents=True, exist_ok=True)

        events_by_run: dict[str, list[DAQEvent]] = {}
        for event in events:
            events_by_run.setdefault(event.run_id, []).append(event)

        event_fieldnames = [
            "event_id",
            "timestamp",
            "run_id",
            "producer_id",
            "source",
            "method",
            "direction",
        ]
        compact_fieldnames = [
            *event_fieldnames,
            "value",
            "unit",
            "integral",
            "integral_unit",
        ]

        for run_id, run_events in events_by_run.items():
            path = Path("data/csv") / f"daq_capture_{_safe_filename_part(run_id)}.csv"
            if self.config.verbose_save:
                rows = [
                    {
                        "event_id": event.event_id,
                        "timestamp": event.timestamp,
                        "run_id": event.run_id,
                        "producer_id": event.producer_id,
                        "source": event.source,
                        "method": event.method,
                        "direction": event.direction,
                        **_flatten_for_csv(event.tags, prefix="tags"),
                        **_flatten_for_csv(event.data, prefix="data"),
                    }
                    for event in run_events
                ]
                existing_rows: list[dict[str, Any]] = []
                existing_fieldnames: list[str] = []
                if path.exists():
                    with path.open("r", encoding="utf-8", newline="") as f:
                        reader = csv.DictReader(f)
                        existing_fieldnames = list(reader.fieldnames or [])
                        existing_rows = list(reader)

                dynamic_fieldnames = sorted(
                    {
                        key
                        for row in [*existing_rows, *rows]
                        for key in row
                        if key not in event_fieldnames
                    }
                )
                fieldnames = event_fieldnames + dynamic_fieldnames
                mode = "w" if existing_fieldnames != fieldnames else "a"
                with path.open(mode, encoding="utf-8", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                    if mode == "w":
                        writer.writeheader()
                        writer.writerows(existing_rows)
                    writer.writerows(rows)
                continue

            rows = [
                _compact_csv_row(event)
                for event in run_events
            ]
            write_header = (not path.exists()) or path.stat().st_size == 0
            with path.open("a", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=compact_fieldnames, extrasaction="ignore")
                if write_header:
                    writer.writeheader()
                writer.writerows(rows)

        self.logger.info("Flushed %d events of type csv", len(events))

    async def _write_hdf5(self, events: list[DAQEvent]) -> None:
        if not self._data_path:
            self.logger.error(
                "[!] Data path not available. Cannot write HDF5.")
            return

        Path("data/hdf5").mkdir(parents=True, exist_ok=True)

        events_by_run: dict[str, list[DAQEvent]] = {}
        for event in events:
            events_by_run.setdefault(event.run_id, []).append(event)

        for run_id, run_events in events_by_run.items():
            path = Path("data/hdf5") / f"daq_capture_{_safe_filename_part(run_id)}.hdf5"
            with h5py.File(path, "a") as f:
                f.attrs["run_id"] = str(run_id)
                for event in run_events:
                    run_group = f.require_group(str(event.run_id))
                    event_key = _unique_hdf5_key(run_group, str(event.event_id))
                    event_group = run_group.create_group(event_key)
                    event_group.attrs["event_id"] = int(event.event_id)
                    event_group.attrs["timestamp"] = str(event.timestamp)
                    event_group.attrs["run_id"] = str(event.run_id)
                    event_group.attrs["producer_id"] = str(event.producer_id)
                    event_group.attrs["source"] = str(event.source)
                    event_group.attrs["method"] = str(event.method)
                    event_group.attrs["direction"] = str(event.direction)
                    if self.config.verbose_save:
                        _write_hdf5_value(event_group, "tags", event.tags)
                        _write_hdf5_value(event_group, "data", event.data)
                    else:
                        for key, value in _compact_event_data(event).items():
                            _write_compact_hdf5_value(event_group, key, value)

        self.logger.info("Flushed %d events of type hdf5", len(events))

    ######################################################################################################
    ################## These functions below are for GUI interaction and manual commits ##################
    ####################### They will not be executed when decorating a function #########################
    ######################################################################################################

    def commit(self, *, source: str, method: str, data: dict, direction: str = "out", run_id: str | None = None, producer_id: str | None = None, metadata: dict | None = None, tags: dict | None = None,) -> int:
        event_data = dict(data)
        normalized_tags = _normalize_tags(tags)
        if metadata:
            event_data["metadata"] = metadata
        if normalized_tags:
            event_data["tags"] = normalized_tags

        pending_event = PendingEvent(
            event_id=0,  # will be set in publish_pending_event
            run_id=run_id or get_run_id(),
            timestamp=str(time.time() * 1000),
            producer_id=producer_id or "default_producer",
            source=source,
            method=method,
            direction=cast(Literal["in", "out", "error"], direction),
            data=event_data,
            tags=normalized_tags,
        )

        self.publish_pending_event(pending_event)

        return pending_event.event_id

    def commit_preview(self, preview: PreviewFrame, *, method: str = "manual_commit", analysis: dict | None = None, user_metadata: dict | None = None, tags: dict | None = None) -> int:
        metadata = {
            **preview.metadata,
            **(user_metadata or {}),
            "preview_timestamp": preview.timestamp,
            "preview_sequence_id": preview.sequence_id,
        }

        return self.commit(
            source=preview.source,
            producer_id=preview.producer_id,
            method=method,
            data={
                "preview": preview.data,
                "analysis": analysis or {},
            },
            metadata=metadata,
            tags=tags,
        )


def _extract_tags_from_data(data: Any) -> dict[str, Any]:
    if not isinstance(data, Mapping):
        return {}
    tags = data.get("tags")
    if not isinstance(tags, Mapping):
        return {}
    return dict(tags)


def _compact_event_data(event: DAQEvent) -> dict[str, Any]:
    value, unit = _extract_value_and_unit(event)
    compact: dict[str, Any] = {}
    if value is not None:
        compact["value"] = value
    if unit:
        compact["unit"] = unit

    integral, integral_unit = _extract_integral(event.data)
    if integral is not None:
        compact["integral"] = integral
    if integral_unit:
        compact["integral_unit"] = integral_unit

    if compact:
        return compact
    return {"value": _compact_fallback_value(event.data)}


def _compact_csv_row(event: DAQEvent) -> dict[str, Any]:
    compact = _compact_event_data(event)
    return {
        "event_id": event.event_id,
        "timestamp": event.timestamp,
        "run_id": event.run_id,
        "producer_id": event.producer_id,
        "source": event.source,
        "method": event.method,
        "direction": event.direction,
        "value": _compact_csv_scalar(compact.get("value")),
        "unit": compact.get("unit", ""),
        "integral": _compact_csv_scalar(compact.get("integral")),
        "integral_unit": compact.get("integral_unit", ""),
    }


def _compact_csv_scalar(value: Any) -> Any:
    if value is None or isinstance(value, bool | int | float | str):
        return value
    return json.dumps(value, default=str)


def _extract_value_and_unit(event: DAQEvent) -> tuple[Any | None, str]:
    if not isinstance(event.data, Mapping):
        return event.data, ""

    preview = event.data.get("preview")
    metadata = event.data.get("metadata")
    unit = _extract_unit(metadata) or _extract_unit(preview) or _default_unit(event)

    if isinstance(preview, Mapping):
        state = preview.get("state")
        if isinstance(state, Mapping):
            if "value" in state:
                return state.get("value"), unit
            corrected = state.get("corrected_field")
            if isinstance(corrected, Mapping):
                return _compact_field_vector(corrected), unit or "G"

        scalar = _first_numeric_scalar(preview)
        if scalar is not None:
            return scalar, unit

    return None, unit


def _extract_unit(value: Any) -> str:
    if not isinstance(value, Mapping):
        return ""
    unit = value.get("unit")
    if isinstance(unit, str):
        return unit
    return ""


def _default_unit(event: DAQEvent) -> str:
    if event.source == "counter":
        return "count"
    if event.source == "magnetic_field":
        return "G"
    return ""


def _compact_field_vector(value: Mapping[str, Any]) -> dict[str, Any]:
    vector: dict[str, Any] = {}
    for compact_key, source_key in (
        ("x", "x_gauss"),
        ("y", "y_gauss"),
        ("z", "z_gauss"),
    ):
        if source_key in value:
            vector[compact_key] = value[source_key]
    return vector or dict(value)


def _first_numeric_scalar(value: Mapping[str, Any]) -> int | float | bool | None:
    for child in value.values():
        if isinstance(child, bool | int | float):
            return child
    return None


def _extract_integral(data: Any) -> tuple[Any | None, str]:
    if not isinstance(data, Mapping):
        return None, ""
    analysis = data.get("analysis")
    if not isinstance(analysis, Mapping):
        return None, ""
    integration = analysis.get("integration")
    if not isinstance(integration, Mapping):
        return None, ""
    series = integration.get("series")
    if isinstance(series, Sequence) and not isinstance(series, str | bytes | bytearray):
        for item in series:
            if isinstance(item, Mapping) and "integral" in item:
                unit = item.get("integral_unit")
                return item.get("integral"), unit if isinstance(unit, str) else ""
    unit = integration.get("integral_unit")
    return integration.get("integral"), unit if isinstance(unit, str) else ""


def _compact_fallback_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        for key in ("value", "state", "preview"):
            if key in value:
                return _compact_fallback_value(value[key])
        return json.dumps(value, default=str)
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return json.dumps(list(value), default=str)
    return value


def _flatten_for_csv(value: Any, *, prefix: str) -> dict[str, Any]:
    if isinstance(value, Mapping):
        flattened: dict[str, Any] = {}
        if not value:
            flattened[prefix] = ""
        for key, child in value.items():
            flattened.update(_flatten_for_csv(child, prefix=f"{prefix}.{key}"))
        return flattened

    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        flattened = {}
        if not value:
            flattened[prefix] = ""
        for index, child in enumerate(value):
            flattened.update(_flatten_for_csv(child, prefix=f"{prefix}.{index}"))
        return flattened

    return {prefix: _csv_scalar(value)}


def _normalize_tags(tags: Mapping[str, Any] | None, *, prefix: str = "") -> dict[str, Any]:
    if not tags:
        return {}

    normalized: dict[str, Any] = {}
    for key, value in tags.items():
        tag_key = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, Mapping):
            normalized.update(_normalize_tags(value, prefix=tag_key))
            continue

        scalar = _tag_scalar(value)
        if scalar is not None:
            normalized[tag_key] = scalar

    return normalized


def _tag_scalar(value: Any) -> str | bool | int | float | None:
    if value is None:
        return None
    if isinstance(value, bool | int | float | str):
        return value
    return str(value)


def _csv_scalar(value: Any) -> Any:
    if value is None or isinstance(value, bool | int | float | str):
        return value
    return str(value)


def _safe_filename_part(value: Any) -> str:
    text = str(value).strip()
    cleaned = [character if character.isalnum() or character in ("-", "_", ".") else "_" for character in text]
    return "".join(cleaned).strip("._") or "unknown"


def _unique_hdf5_key(group, base_key: str) -> str:
    event_key = base_key
    if event_key not in group:
        return event_key
    suffix = 1
    while f"{base_key}_{suffix}" in group:
        suffix += 1
    return f"{base_key}_{suffix}"


def _safe_hdf5_name(name: Any) -> str:
    text = str(name).replace("/", "_").strip()
    return text or "value"


def _write_hdf5_value(group, name: str, value: Any) -> None:
    safe_name = _safe_hdf5_name(name)
    if isinstance(value, Mapping):
        child_group = group.create_group(safe_name)
        if safe_name != str(name):
            child_group.attrs["original_name"] = str(name)
        if not value:
            child_group.attrs["empty"] = True
        for key, child in value.items():
            _write_hdf5_value(child_group, str(key), child)
        return

    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        if _is_flat_hdf5_sequence(value):
            data = _flat_hdf5_sequence_data(value)
            if all(isinstance(item, str) for item in data):
                group.create_dataset(safe_name, data=data, dtype=h5py.string_dtype("utf-8"))
            else:
                group.create_dataset(safe_name, data=data)
            return
        child_group = group.create_group(safe_name)
        if not value:
            child_group.attrs["empty"] = True
        for index, child in enumerate(value):
            _write_hdf5_value(child_group, str(index), child)
        return

    if value is None:
        dataset = group.create_dataset(safe_name, data="")
        dataset.attrs["is_none"] = True
        return

    if isinstance(value, str):
        group.create_dataset(safe_name, data=value, dtype=h5py.string_dtype("utf-8"))
        return

    if isinstance(value, bytes | bytearray):
        group.create_dataset(safe_name, data=bytes(value))
        return

    group.create_dataset(safe_name, data=value)


def _write_compact_hdf5_value(group, name: str, value: Any) -> None:
    if value is None:
        return
    safe_name = _safe_hdf5_name(name)
    if isinstance(value, str):
        group.create_dataset(safe_name, data=value, dtype=h5py.string_dtype("utf-8"))
        return
    if isinstance(value, Mapping) or (
        isinstance(value, Sequence) and not isinstance(value, bytes | bytearray)
    ):
        group.create_dataset(
            safe_name,
            data=json.dumps(value, default=str),
            dtype=h5py.string_dtype("utf-8"),
        )
        return
    if isinstance(value, bytes | bytearray):
        group.create_dataset(safe_name, data=bytes(value))
        return
    group.create_dataset(safe_name, data=value)


def _is_flat_hdf5_sequence(value: Sequence[Any]) -> bool:
    return all(item is None or isinstance(item, bool | int | float | str) for item in value)


def _flat_hdf5_sequence_data(value: Sequence[Any]) -> list[Any]:
    if any(item is None or isinstance(item, str) for item in value):
        return ["" if item is None else str(item) for item in value]
    return list(value)
