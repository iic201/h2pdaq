from __future__ import annotations

import asyncio
import json
import logging

import grpc

from h2pcontrol.central_daq.v1.central_daq_pb2 import (
    StreamDAQEventsRequest,
    StreamDAQEventsResponse,
)
from h2pcontrol.central_daq.v1.central_daq_pb2_grpc import (
    CentralDAQServiceStub,
)

from ..models import DAQEvent


class GrpcDAQSink:
    def __init__(
        self,
        central_address: str,
        queue: asyncio.Queue[DAQEvent],
        logger: logging.Logger | None = None,
    ) -> None:
        self.central_address = central_address
        self.queue = queue
        self.logger = logger or logging.getLogger(__name__)
        self._stopping = False

    async def stop(self) -> None:
        self._stopping = True

    async def run(self) -> None:
        while not self._stopping:
            try:
                await self._stream_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.error(
                    "[C] gRPC stream to central DAQ failed; reconnecting soon"
                )
                await asyncio.sleep(2.0)

    async def _stream_once(self) -> None:
        async with grpc.aio.insecure_channel(
            self.central_address,
            options=[
                ("grpc.max_receive_message_length", 50 * 1024 * 1024),
                ("grpc.max_send_message_length", 50 * 1024 * 1024),
            ],
        ) as channel:
            stub = CentralDAQServiceStub(channel)

            response = await stub.StreamDAQEvents(self._event_generator())

            self.logger.info(
                "[C] Central DAQ stream closed: received=%s message=%s",
                response.received,
                response.message,
            )

    async def _event_generator(self):
        while not self._stopping:
            event = await self.queue.get()

            try:
                yield StreamDAQEventsRequest(
                    event_id=event.event_id,
                    timestamp=event.timestamp,
                    run_id=event.run_id,
                    producer_id=event.producer_id,
                    source=event.source,
                    method=event.method,
                    direction=event.direction,
                    data=json.dumps(event.data, default=str),
                )
            finally:
                self.queue.task_done()
