from __future__ import annotations

import asyncio
import logging
import json
import time
import csv
import h5py
from typing import cast, Literal
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
import aiofiles

# TODO: Use batching writes for CSV and JSONL to improve performance.

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
        self._outbound_jsonl_q: asyncio.Queue[DAQEvent] = asyncio.Queue(
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
            asyncio.create_task(self._outbound_jsonl_loop()),
            asyncio.create_task(self._outbound_influx_loop()),
            asyncio.create_task(self._outbound_hdf5_loop()),
        ]

    async def stop(self) -> None:
        self.logger.info("[Central-DAQ] Stopping service..")
        await self._ingest_q.join()
        await self._outbound_csv_q.join()
        await self._outbound_jsonl_q.join()
        await self._outbound_influx_q.join()
        await self._outbound_hdf5_q.join()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await asyncio.to_thread(self._influx.close)
        self.logger.info("[Central-DAQ] Service stopped. Having received %s events and having dropped %s events in the ingress queue."
                         , self.stats.received, self.stats.dropped_ingress)
        self.logger.info("[Central-DAQ] In the outbound queues, having dropped %s events in the CSV queue, %s events in the JSONL queue, %s events in the HDF5 queue, %s events in the InfluxDB queue.",
                         self.stats.dropped_outbound_csv, self.stats.dropped_outbound_jsonl, self.stats.dropped_outbound_hdf5, self.stats.dropped_outbound_influx)


    async def accept_event(self, event: StreamDAQEventsRequest) -> None:
        if self._data_path_created:
            self._create_path_for_source("data/{source}".format(source=event.source))
            
        self.stats.ingested += 1
        # TODO: Apply overflow policy for this put
        await self._ingest_q.put(event)

    async def StreamDAQEvents(self, request_iterator, context):
        async for event in request_iterator:
            await self.accept_event(event)
            self.stats.received += 1

        response = StreamDAQEventsResponse(
            received=200,
            message="Received {received} events".format(received=self.stats.received),
        )

        self.logger.info(
            "[Central-DAQ] Stream closed: received=200 message=%s",
            response.message
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
                daq_event = DAQEvent(
                    event_id=event.event_id,
                    timestamp=event.timestamp,
                    run_id=event.run_id,
                    producer_id=event.producer_id,
                    source=event.source,
                    method=event.method,
                    direction=cast(Literal["in", "out", "error"], event.direction),
                    data=event.data,
                )
                await self._outbound_csv_q.put(daq_event)
                await self._outbound_jsonl_q.put(daq_event)
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
        
        path = "data/{source}/csv".format(source=event.source)
        if not Path(path).is_dir():
            self._create_path_for_data_type(event.source, "csv")
            self.logger.info("[Central-DAQ] Path does not exist, creating one: %s", path)

        if not Path(path).is_dir():
            self.logger.error("[Central-DAQ] Failed to create path for CSV: %s", path)
            return
        
        file_path = "{path}/{producer_id}.csv".format(path=path, producer_id=event.producer_id)
        file_path_obj = Path(file_path)
        write_header = (not file_path_obj.exists()) or file_path_obj.stat().st_size == 0

        with open(file_path, "a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "event_id",
                "timestamp",
                "run_id",
                "producer_id",
                "source",
                "method",
                "direction",
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
                "data": event.data,
            })

    async def _outbound_jsonl_loop(self) -> None:
        while True:
            event = await self._outbound_jsonl_q.get()
            try:
                await self._write_jsonl(event)
            except Exception as e:
                self.stats.dropped_outbound_jsonl += 1
                self.logger.error(
                    "[Central-DAQ] Failed to write JSONL for event_id=%s: %s",
                    event.event_id,
                    str(e),
                )
            finally:
                self._outbound_jsonl_q.task_done()


    async def _write_jsonl(self, event: DAQEvent) -> None:
        if not self._data_path_created:
            self.logger.error("[Central-DAQ] Data path not created; cannot write JSONL")
            return
        
        path = "data/{source}/jsonl".format(source=event.source)
        if not Path(path).is_dir():
            self._create_path_for_data_type(event.source, "jsonl")
            self.logger.info("[Central-DAQ] Path does not exist, creating one: %s", path)
        
        if not Path(path).is_dir():
            self.logger.error("[Central-DAQ] Failed to create path for JSONL: %s", path)
            return
        
        file_path = "{path}/{producer_id}.jsonl".format(path=path, producer_id=event.producer_id)

        json_line = json.dumps({
            "event_id": event.event_id,
            "timestamp": event.timestamp,
            "run_id": event.run_id,
            "producer_id": event.producer_id,
            "source": event.source,
            "method": event.method,
            "direction": event.direction,
            "data": event.data,
        }, default=str)

        async with aiofiles.open(file_path, "a", encoding="utf-8") as f:
            await f.write(json_line + "\n")

        self.logger.info(
            "[Central-DAQ] Written JSONL for event_id=%s to %s",
            event.event_id,
            file_path,
        )


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
        
        path = "data/{source}/hdf5".format(source=event.source)
        if not Path(path).is_dir():
            self._create_path_for_data_type(event.source, "hdf5")
            self.logger.info("[Central-DAQ] Path does not exist, creating one: %s", path)

        if not Path(path).is_dir():
            self.logger.error("[Central-DAQ] Failed to create path for HDF5: %s", path)
            return
        
        file_path = "{path}/{source}.hdf5".format(path=path, source=event.source)
        with h5py.File(file_path, "a") as f:
            producers_group = f.require_group("producers")
            producer_group = producers_group.require_group(str(event.producer_id))
            events_group = producer_group.require_group("events")
            group_name = str(event.event_id)
            if group_name in events_group:
                group_name = "{event_id}_{timestamp}_{nonce}".format(
                    event_id=event.event_id,
                    timestamp=event.timestamp,
                    nonce=time.time_ns(),
                )
            event_group = events_group.create_group(group_name)
            event_group.attrs["timestamp"] = event.timestamp
            event_group.attrs["run_id"] = event.run_id
            event_group.attrs["producer_id"] = event.producer_id
            event_group.attrs["source"] = event.source
            event_group.attrs["method"] = event.method
            event_group.attrs["direction"] = event.direction
            # Assuming data is JSON-serializable; if not, this will need to be adapted.
            event_group.create_dataset("data", data=json.dumps(event.data, default=str))

    def _create_path_for_source(self, path: str) -> bool:
        Path(path).mkdir(parents=True, exist_ok=True)
        return True
    
    def _create_path_for_data_type(self, source: str, data_type: str) -> bool:
        path = "data/{source}/{data_type}".format(source=source, data_type=data_type)
        Path(path).mkdir(parents=True, exist_ok=True)
        return True
