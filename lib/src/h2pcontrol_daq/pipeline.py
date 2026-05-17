from __future__ import annotations
import asyncio
import contextlib
import time
import json
import h5py
import logging
import csv
from .models import DAQEvent, PendingEvent, OverflowPolicy, DAQConfig, LocalDAQStats
from .centralDAQ_connector.grpc_central_sink import GrpcDAQSink
from pathlib import Path

class LocalDAQ:
    def __init__(self, config: DAQConfig | None = None) -> None:
        self.config = config if config is not None else DAQConfig()
        self.stats = LocalDAQStats()
        self.logger = self.setup_logger()

        self._ingress_q: asyncio.Queue[PendingEvent] = asyncio.Queue(
            maxsize=self.config.ingress_maxsize
        )
        self._outbound_jsonl_q: asyncio.Queue[DAQEvent] = asyncio.Queue(
            maxsize=self.config.outbound_maxsize
        )
        self._outbound_hdf5_q: asyncio.Queue[DAQEvent] = asyncio.Queue(
            maxsize=self.config.outbound_maxsize
        )
        self._outbound_csv_q: asyncio.Queue[DAQEvent] = asyncio.Queue(
            maxsize=self.config.outbound_maxsize
        )
        self._outbound_central_q: asyncio.Queue[DAQEvent] = asyncio.Queue(
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

    async def start(self) -> None:
        self.logger.info("Starting DAQ with config: %s", self.config)
        # Create one worker for the serializer and one each for the writers.
        self._tasks = [
            asyncio.create_task(self._serializer_loop(), name="daq-serializer"),
            asyncio.create_task(self._writer_jsonl_loop(), name="daq-writer-jsonl"),
            asyncio.create_task(self._writer_hdf5_loop(), name="daq-writer-hdf5"),
            asyncio.create_task(self._writer_csv_loop(), name="daq-writer-csv"),
        ]

        if self.config.enable_central_stream:
            self.logger.info("[C] Central stream enabled. Starting central sink task.")
            self._tasks.append(
                asyncio.create_task(self.central_sink.run(), name="daq-central-sink")
            )

    async def stop(self) -> None:
        self.logger.info("Stopping DAQ having published %s events. Having dropped %s ingress events and %s outbound events.\n", 
                         self.stats.published, self.stats.dropped_ingress, self.stats.dropped_outbound_jsonl + self.stats.dropped_outbound_hdf5 + self.stats.dropped_outbound_csv)
        self._stopping = True
        # Wait for queues to be fully processed before cancelling tasks.
        await self._ingress_q.join()
        await self._outbound_jsonl_q.join()
        await self._outbound_hdf5_q.join()
        await self._outbound_csv_q.join()
        await self._outbound_central_q.join()
        # Cancel any remaining tasks
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def _create_data_path(self) -> bool:
        Path("data").mkdir(parents=True, exist_ok=True)
        if Path("data").exists():
            return True
        else:
            self.logger.error("Failed to create data directory at path: %s", Path("data"))
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

    async def _serializer_loop(self) -> None:
        while True:
            pending = await self._get_or_stop_pending(self._ingress_q, timeout=0.2)
            if pending is None:
                if self._stopping:
                    break
                continue

            try:
                event = DAQEvent(
                    event_id=pending.event_id,
                    run_id=pending.run_id,
                    timestamp=pending.timestamp,
                    producer_id=pending.producer_id,
                    source=pending.source,
                    method=pending.method,
                    direction=pending.direction,
                    data=pending.data,
                )
                self.stats.serialized += 1
                self._queue_put_now(
                    self._outbound_jsonl_q,
                    event,
                    self.config.outbound_overflow,
                    "outbound_jsonl",
                )
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
                    self.logger.info("[C] Central stream enabled. Queuing event.")
                    self._queue_put_now(
                        self._outbound_central_q,
                        event,
                        self.config.outbound_overflow,
                        "outbound_central",
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
                    self.logger.exception("Failed to write batch of %d events", len(batch))
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
                    self.logger.exception("Failed to write batch of %d events", len(batch))
                finally:
                    for _ in batch:
                        self._outbound_hdf5_q.task_done()
                    batch.clear()
                    last_flush = time.monotonic()

            if self._stopping and self._outbound_hdf5_q.empty() and not batch:
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
        file_handler = logging.FileHandler(".logs/daq-info.log", encoding="utf-8")
        error_file_handler = logging.FileHandler(".logs/daq-error.log", encoding="utf-8")
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
            self.logger.error("[!] Data path not available. Cannot write JSONL.")
            return
        
        Path("data/jsonl").mkdir(parents=True, exist_ok=True)

        for event in events:
            path = Path("data/jsonl") / f"daq_capture_{event.producer_id}.jsonl"
            record = {
                "event_id": event.event_id,
                "run_id": event.run_id,
                "producer_id": event.producer_id,
                "source": event.source,
                "method": event.method,
                "direction": event.direction,
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

        for event in events:
            path = Path("data/csv") / f"daq_capture_{event.producer_id}.csv"
            file_exists = path.exists()
            with path.open("a", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=[
                    "event_id",
                    "run_id",
                    "producer_id",
                    "source",
                    "method",
                    "direction",
                    "message",
                ])
                if not file_exists:
                    writer.writeheader()
                writer.writerow({
                    "event_id": event.event_id,
                    "run_id": event.run_id,
                    "producer_id": event.producer_id,
                    "source": event.source,
                    "method": event.method,
                    "direction": event.direction,
                    "message": json.dumps(event.data, default=str),
                })
            

        self.logger.info("Flushed %d events of type csv", len(events))

    async def _write_hdf5(self, events: list[DAQEvent]) -> None:
        if not self._data_path:
            self.logger.error("[!] Data path not available. Cannot write HDF5.")
            return
        
        Path("data/hdf5").mkdir(parents=True, exist_ok=True)

        events_by_producer: dict[str, list[DAQEvent]] = {}
        for event in events:
            events_by_producer.setdefault(event.producer_id, []).append(event)

        for producer_id, producer_events in events_by_producer.items():
            path = Path("data/hdf5") / f"daq_capture_{producer_id}.hdf5"
            with h5py.File(path, "a") as f:
                dt = h5py.string_dtype(encoding="utf-8")
                for event in producer_events:
                    group = f.require_group(str(event.run_id))
                    base_key = str(event.event_id)
                    event_key = base_key
                    if event_key in group:
                        suffix = 1
                        while f"{base_key}_{suffix}" in group:
                            suffix += 1
                        event_key = f"{base_key}_{suffix}"

                    record = {
                        "event_id": event.event_id,
                        "run_id": event.run_id,
                        "producer_id": event.producer_id,
                        "source": event.source,
                        "method": event.method,
                        "direction": event.direction,
                        "message": json.dumps(event.data, default=str),
                    }
                    dset = group.create_dataset(event_key, (1,), dtype=dt)
                    dset[0] = json.dumps(record, default=str)

        self.logger.info("Flushed %d events of type hdf5", len(events))








        
