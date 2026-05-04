"""Minimal central-side pipeline and event model.

This module is intentionally separate from lib/daq_pipeline.py.
Producers keep using the library pipeline, while daq-central owns this local
copy of the event schema and a tiny relay that subscribes to the producer UI
broadcaster and forwards events to the central ingest layer.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class DAQEvent:
    """Central-side event record copied from the producer event shape."""

    timestamp_ns: int
    run_id: str
    source: str
    method: str
    direction: str
    sequence: int
    payload: dict[str, Any]
    units: dict[str, str] = field(default_factory=dict)
    quality: int = 0


class CentralPipeline:
    """Relay events from a producer UI broadcaster into central subscribers."""

    def __init__(self, source_pipeline: Any | None = None, run_id: str | None = None):
        self.source_pipeline = source_pipeline
        self.run_id = run_id or str(uuid.uuid4())
        self.ui_clients: set = set()
        self._source_queue: asyncio.Queue[DAQEvent] | None = None
        self._source_task: asyncio.Task | None = None
        self._stopping = False

    def subscribe(self, client) -> None:
        self.ui_clients.add(client)

    def unsubscribe(self, client) -> None:
        self.ui_clients.discard(client)

    async def start(self) -> None:
        """Subscribe to the producer UI broadcaster and start relaying."""
        if self.source_pipeline is None or self._source_task is not None:
            return

        self._source_queue = asyncio.Queue()
        self._subscribe_to_source(self._source_queue)
        self._source_task = asyncio.create_task(self._relay_source_events(), name="central-relay")

    async def stop(self) -> None:
        """Detach from the producer UI broadcaster and stop relaying."""
        self._stopping = True

        if self._source_queue is not None:
            self._unsubscribe_from_source(self._source_queue)
            await self._source_queue.join()

        if self._source_task is not None:
            self._source_task.cancel()
            await asyncio.gather(self._source_task, return_exceptions=True)

        self._source_task = None
        self._source_queue = None
        self._stopping = False

    async def publish(self, event: DAQEvent) -> None:
        """Fan out an event to local central subscribers."""
        for client in tuple(self.ui_clients):
            await client.put(event)

    def _subscribe_to_source(self, queue: asyncio.Queue[DAQEvent]) -> None:
        if hasattr(self.source_pipeline, "subscribe"):
            self.source_pipeline.subscribe(queue)
        else:
            self.source_pipeline.ui_clients.add(queue)

    def _unsubscribe_from_source(self, queue: asyncio.Queue[DAQEvent]) -> None:
        if hasattr(self.source_pipeline, "unsubscribe"):
            self.source_pipeline.unsubscribe(queue)
        else:
            self.source_pipeline.ui_clients.discard(queue)

    async def _relay_source_events(self) -> None:
        while True:
            try:
                event = await asyncio.wait_for(self._source_queue.get(), timeout=0.2)
            except asyncio.TimeoutError:
                if self._stopping and self._source_queue.empty():
                    break
                continue

            try:
                await self.publish(event)
            finally:
                self._source_queue.task_done()

            if self._stopping and self._source_queue.empty():
                break

    


DAQPipeline = CentralPipeline


__all__ = ["DAQEvent", "DAQPipeline", "CentralPipeline"]