from __future__ import annotations

import asyncio
import logging
import json
import time
import csv
import h5py
from typing import Any, cast, Literal
from pathlib import Path
from h2pcontrol.central_daq.v1.central_daq_pb2 import (
    StreamDAQEventsRequest,
    StreamDAQEventsResponse,
)
from h2pcontrol.central_daq.v1.central_daq_pb2_grpc import (
    CentralDAQServiceServicer,
)
from .influxdb import InfluxDBClient, InfluxDBConfig
from .models import DAQEvent, CentralDAQConfig, CentralDAQStats

class CentralDAQService(CentralDAQServiceServicer):
    def __init__(self) -> None:
        self.logger = self.setup_logger()
        self.stats = CentralDAQStats()
        self.config = CentralDAQConfig()

        self._ingest_q: asyncio.Queue[StreamDAQEventsRequest] = asyncio.Queue(
            maxsize=self.config.ingest_maxsize
        )
        self._outbound_csv_q: asyncio.Queue[DAQEvent] = asyncio.Queue(
            maxsize=self.config.outbound_maxsize
        )
        self._outbound_influx_q: asyncio.Queue[DAQEvent] = asyncio.Queue(
            maxsize=self.config.outbound_maxsize
        )
        self._outbound_hdf5_q: asyncio.Queue[DAQEvent] = asyncio.Queue(
            maxsize=self.config.outbound_maxsize
        )

        self._influx = InfluxDBClient(InfluxDBConfig(), logger=self.logger)

        self._data_path_created = self._data_path()
        self._stopping = False

        self.tasks: list[asyncio.Task] = []

    def _data_path(self) -> bool:
        Path("data").mkdir(parents=True, exist_ok=True)
        if not Path("data").is_dir():
            self.logger.error("[Central-DAQ] Failed to create data directory")
            return False
        return True

    async def start(self) -> None:
        self.logger.info("[Central-DAQ] Starting with config: %s", self.config)
        self.tasks = [
            asyncio.create_task(self._ingest_loop()),
            asyncio.create_task(self._outbound_csv_loop()),
            asyncio.create_task(self._outbound_influx_loop()),
            asyncio.create_task(self._outbound_hdf5_loop()),
        ]

    async def stop(self) -> None:
        self.logger.info("[Central-DAQ] Stopping service..")
        await self._ingest_q.join()
        await self._outbound_csv_q.join()
        await self._outbound_influx_q.join()
        await self._outbound_hdf5_q.join()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await asyncio.to_thread(self._influx.close)
        self.logger.info("[Central-DAQ] Service stopped. Having received %s events and having dropped %s events in the ingress queue."
                         , self.stats.received, self.stats.dropped_ingress)
        self.logger.info("[Central-DAQ] In the outbound queues, having dropped %s events in the CSV queue, %s events in the HDF5 queue, %s events in the InfluxDB queue.",
                         self.stats.dropped_outbound_csv, self.stats.dropped_outbound_hdf5, self.stats.dropped_outbound_influx)


    async def accept_event(self, event: StreamDAQEventsRequest) -> None:
        if self._data_path_created:
            self._create_path_for_source(event.source)
            
        self.stats.ingested += 1
        # TODO: Apply overflow policy for this put
        await self._ingest_q.put(event)

    async def StreamDAQEvents(self, request_iterator, context):
        count = 0
        async for event in request_iterator:
            await self.accept_event(event)
            count += 1

        response = StreamDAQEventsResponse(
            received=count,
            message="Received {received} events".format(received=count),
        )

        self.logger.info(
            "[Central-DAQ] Stream closed: received=%s message=%s",
            response.received,
            response.message,
        )
        return response
    
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
    
    async def _ingest_loop(self) -> None:
        while True:
            event = await self._ingest_q.get()
            try:
                data = _decode_event_data(event.data)
                daq_event = DAQEvent(
                    event_id=event.event_id,
                    timestamp=event.timestamp,
                    run_id=event.run_id,
                    producer_id=event.producer_id,
                    source=event.source,
                    method=event.method,
                    direction=cast(Literal["in", "out", "error"], event.direction),
                    data=data,
                    tags=_extract_tags(data),
                )
                await self._outbound_csv_q.put(daq_event)
                await self._outbound_influx_q.put(daq_event)
                await self._outbound_hdf5_q.put(daq_event)
            except Exception as e:
                self.stats.dropped_ingress += 1
                self.logger.error(
                    "[Central-DAQ] Failed to process event_id=%s: %s",
                    event.event_id,
                    str(e),
                )
            finally:
                self._ingest_q.task_done()

    async def _outbound_csv_loop(self) -> None:
        while True:
            event = await self._outbound_csv_q.get()
            try:
                await asyncio.to_thread(self._write_csv, event)
            except Exception as e:
                self.stats.dropped_outbound_csv += 1
                self.logger.error(
                    "[Central-DAQ] Failed to write CSV for event_id=%s: %s",
                    event.event_id,
                    str(e),
                )
            finally:
                self._outbound_csv_q.task_done()

    def _write_csv(self, event: DAQEvent) -> None:
        if not self._data_path_created:
            self.logger.error("[Central-DAQ] Data path not created; cannot write CSV")
            return
        
        path = self._data_type_path(event.source, "csv")
        if not path.is_dir():
            self._create_path_for_data_type(event.source, "csv")
            self.logger.info("[Central-DAQ] Path does not exist, creating one: %s", path)

        if not path.is_dir():
            self.logger.error("[Central-DAQ] Failed to create path for CSV: %s", path)
            return
        
        file_path_obj = path / f"{_safe_path_part(event.producer_id)}.csv"
        write_header = (not file_path_obj.exists()) or file_path_obj.stat().st_size == 0

        with file_path_obj.open("a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "event_id",
                "timestamp",
                "run_id",
                "producer_id",
                "source",
                "method",
                "direction",
                "tags",
                "data",
            ])
            if write_header:
                writer.writeheader()
            writer.writerow({
                "event_id": event.event_id,
                "timestamp": event.timestamp,
                "run_id": event.run_id,
                "producer_id": event.producer_id,
                "source": event.source,
                "method": event.method,
                "direction": event.direction,
                "tags": json.dumps(event.tags, default=str),
                "data": json.dumps(event.data, default=str),
            })

    async def _outbound_influx_loop(self) -> None:
        while True:
            event = await self._outbound_influx_q.get()
            try:
                await asyncio.to_thread(self._influx.write_event, event)
            except Exception as e:
                self.stats.dropped_outbound_influx += 1
                self.logger.error(
                    "[Central-DAQ] Failed to write InfluxDB for event_id=%s: %s",
                    event.event_id,
                    str(e),
                )
            finally:
                self._outbound_influx_q.task_done()

    async def _outbound_hdf5_loop(self) -> None:
        while True:
            event = await self._outbound_hdf5_q.get()
            try:
                await asyncio.to_thread(self._write_hdf5, event)
            except Exception as e:
                self.stats.dropped_outbound_hdf5 += 1
                self.logger.error(
                    "[Central-DAQ] Failed to write HDF5 for event_id=%s: %s",
                    event.event_id,
                    str(e),
                )
            finally:
                self._outbound_hdf5_q.task_done()

    def _write_hdf5(self, event: DAQEvent) -> None:
        if not self._data_path_created:
            self.logger.error("[Central-DAQ] Data path not created; cannot write HDF5")
            return
        
        path = self._data_type_path(event.source, "hdf5")
        if not path.is_dir():
            self._create_path_for_data_type(event.source, "hdf5")
            self.logger.info("[Central-DAQ] Path does not exist, creating one: %s", path)

        if not path.is_dir():
            self.logger.error("[Central-DAQ] Failed to create path for HDF5: %s", path)
            return
        
        file_path = path / f"{_safe_path_part(event.source)}.hdf5"
        with h5py.File(file_path, "a") as f:
            producers_group = f.require_group("producers")
            producer_group = producers_group.require_group(_safe_hdf5_name(event.producer_id))
            producer_group.attrs["producer_id"] = str(event.producer_id)
            events_group = producer_group.require_group("events")
            group_name = _safe_hdf5_name(event.event_id)
            if group_name in events_group:
                group_name = _safe_hdf5_name("{event_id}_{timestamp}_{nonce}".format(
                    event_id=event.event_id,
                    timestamp=event.timestamp,
                    nonce=time.time_ns(),
                ))
            event_group = events_group.create_group(group_name)
            event_group.attrs["timestamp"] = event.timestamp
            event_group.attrs["run_id"] = event.run_id
            event_group.attrs["producer_id"] = event.producer_id
            event_group.attrs["source"] = event.source
            event_group.attrs["method"] = event.method
            event_group.attrs["direction"] = event.direction
            event_group.attrs["tags"] = json.dumps(event.tags, default=str)
            # Assuming data is JSON-serializable; if not, this will need to be adapted.
            event_group.create_dataset("data", data=json.dumps(event.data, default=str))

    def _source_path(self, source: str) -> Path:
        return Path("data") / _safe_path_part(source)

    def _data_type_path(self, source: str, data_type: str) -> Path:
        return self._source_path(source) / _safe_path_part(data_type)

    def _create_path_for_source(self, source: str) -> bool:
        self._source_path(source).mkdir(parents=True, exist_ok=True)
        return True
    
    def _create_path_for_data_type(self, source: str, data_type: str) -> bool:
        self._data_type_path(source, data_type).mkdir(parents=True, exist_ok=True)
        return True


def _decode_event_data(value: str) -> Any:
    if not value:
        return {}
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _extract_tags(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    tags = data.get("tags")
    if not isinstance(tags, dict):
        return {}
    return dict(tags)


def _safe_path_part(value: Any) -> str:
    text = str(value).strip()
    cleaned = [
        character if character.isalnum() or character in ("-", "_") else "_"
        for character in text
    ]
    return "".join(cleaned).strip("_") or "unknown"


def _safe_hdf5_name(value: Any) -> str:
    return _safe_path_part(value)
