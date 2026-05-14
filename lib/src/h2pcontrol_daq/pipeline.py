from __future__ import annotations
import asyncio
import contextlib
import time
import json
from pathlib import Path

import logging
from .models import DAQEvent, PendingEvent, OverflowPolicy, DAQConfig, LocalDAQStats


class LocalDAQ:
    def __init__(self):
        self.config = DAQConfig()
        self.stats = LocalDAQStats()
        self.logger = self.setup_logger()

        self._ingress_q: asyncio.Queue[PendingEvent] = asyncio.Queue(
            maxsize=self.config.ingress_maxsize
        )
        self._outbound_q: asyncio.Queue[DAQEvent] = asyncio.Queue(
            maxsize=self.config.outbound_maxsize
        )
        self._tasks: list[asyncio.Task] = []
        self._stopping = False

    async def start(self) -> None:
        self.logger.info("Starting DAQ with config: %s", self.config)
        # Create one worker for the serializer and one for the writer.
        self._tasks = [
            asyncio.create_task(self._serializer_loop(), name="daq-serializer"),
            asyncio.create_task(self._writer_loop(), name="daq-writer"),
        ]

    async def stop(self) -> None:
        self.logger.info("Stopping DAQ")
        self._stopping = True
        # Wait for queues to be fully processed before cancelling tasks.
        await self._ingress_q.join()
        await self._outbound_q.join()
        # Cancel any remaining tasks
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def publish_pending_event(self, event: PendingEvent) -> None:
        self.logger.info("Publishing event: %s", event)
        self.stats.published += 1
        self._queue_put_now(self._ingress_q, event,
                            self.config.ingress_overflow, "ingress")

    def _queue_put_now(self, q: asyncio.Queue, item, policy: OverflowPolicy, queue_name: str,) -> None:
        self.logger.info("Putting item into %s queue with policy %s: %s", queue_name, policy, item)
        if not q.full():
            q.put_nowait(item)
            return

        if queue_name == "ingress":
            counter_name = "dropped_ingress"
        elif queue_name == "outbound":
            counter_name = "dropped_outbound"

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
        self.logger.info("Attempting to put item into queue with timeout: %s", item)
        try:
            await asyncio.wait_for(q.put(item), timeout=self.config.queue_put_timeout_s)
        except asyncio.TimeoutError:
            setattr(self.stats, counter_name, getattr(self.stats, counter_name) + 1)

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
                    producer_id=pending.producer_id,
                    source=pending.source,
                    method=pending.method,
                    direction=pending.direction,
                    data=pending.data,
                )
                self.stats.serialized += 1
                self._queue_put_now(
                    self._outbound_q,
                    event,
                    self.config.outbound_overflow,
                    "outbound",
                )
            except Exception:
                self.stats.serialization_errors += 1
            finally:
                self._ingress_q.task_done()

    async def _writer_loop(self) -> None:
        batch: list[DAQEvent] = []
        last_flush = time.monotonic()

        while True:
            event = await self._get_or_stop_daq_event(self._outbound_q, timeout=0.5)
            if event is not None:
                batch.append(event)

            now = time.monotonic()
            flush_due = batch and (now - last_flush >= 1.0)
            flush_on_stop = batch and self._stopping and self._outbound_q.empty()

            if flush_due or flush_on_stop:
                try:
                    await self._write_jsonl(batch)
                except Exception:
                    self.logger.exception("Failed to write batch of %d events", len(batch))
                finally:
                    for _ in batch:
                        self._outbound_q.task_done()
                    batch.clear()
                    last_flush = time.monotonic()

            if self._stopping and self._outbound_q.empty() and not batch:
                break


    async def _get_or_stop_pending(self, queue: asyncio.Queue, timeout: float) -> PendingEvent | None:
        self.logger.info("Attempting to get item from queue (%s) with timeout: %f", queue, timeout)
        try:
            return await asyncio.wait_for(queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
        
    async def _get_or_stop_daq_event(self, queue: asyncio.Queue, timeout: float) -> DAQEvent | None:
        self.logger.info("Attempting to get item from queue (%s) with timeout: %f", queue, timeout)
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

        Path("logs").mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler("logs/app.log", encoding="utf-8")
        file_handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)

        return logger
    
    async def _write_jsonl(self, events: list[DAQEvent]) -> None:
        Path("data").mkdir(parents=True, exist_ok=True)

        for event in events:
            path = Path("data") / f"daq_capture_{event.producer_id}.jsonl"
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

        self.logger.info("Flushed %d events", len(events))







        
