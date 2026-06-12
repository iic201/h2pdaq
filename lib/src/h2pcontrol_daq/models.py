from __future__ import annotations
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Literal

@dataclass(slots=True)
class PendingEvent:
    event_id: int
    timestamp: str
    run_id: str
    producer_id: str
    source: str
    method: str
    direction: Literal["in", "out", "error"]
    data: Any
    tags: dict[str, Any] = field(default_factory=dict)
    save_formats: tuple[DAQSaveFormat | str, ...] | None = None
    normalized: bool = False

@dataclass(frozen=True, slots=True)
class DAQEvent:
    event_id: int
    timestamp: str
    run_id: str
    producer_id: str
    source: str
    method: str
    direction: Literal["in", "out", "error"]
    data: Any
    tags: dict[str, Any] = field(default_factory=dict)
    save_formats: tuple[DAQSaveFormat, ...] = field(default_factory=tuple)
    
@dataclass(slots=True)
class LocalDAQStats:
    published: int = 0
    serialized: int = 0
    dropped_ingress: int = 0
    dropped_outbound_csv: int = 0
    dropped_outbound_hdf5: int = 0
    dropped_outbound_central: int = 0
    dropped_outbound_influx: int = 0
    serialization_errors: int = 0

class OverflowPolicy(StrEnum):
    DROP_NEWEST = "drop_newest"
    DROP_OLDEST = "drop_oldest"
    BLOCK_WITH_TIMEOUT = "block_with_timeout"

class DAQSaveFormat(StrEnum):
    CSV = "csv"
    HDF5 = "hdf5"
    INFLUX = "influx"

@dataclass(slots=True)
class DAQConfig:
    ingress_maxsize: int = 10_000 # Maximum number of PendingEvent in the ingress queue before applying overflow policy.
    outbound_maxsize: int = 10_000 # Maximum number of serialized events in the outbound queue before applying overflow policy.
    queue_put_timeout_s: float = 0.1 # Timeout in seconds for blocking a put operation to the queue when using BLOCK_WITH_TIMEOUT policy.
    writer_batch_size: int = 1_000 # Maximum number of events to write in one CSV/HDF5 batch.
    writer_flush_interval_s: float = 1.0 # Maximum time to hold a partial CSV/HDF5 batch before flushing.
    central_flush_timeout_s: float = 2.0 # Maximum shutdown wait for best-effort central streaming.
    ingress_overflow: OverflowPolicy = OverflowPolicy.DROP_NEWEST
    outbound_overflow: OverflowPolicy = OverflowPolicy.DROP_NEWEST
    verbose_save: bool = False # Keep full expanded CSV/HDF5 payloads instead of compact event summaries.
    save_formats: tuple[DAQSaveFormat | str, ...] = (DAQSaveFormat.CSV,) # Save formats for local DAQ
    enable_central_stream: bool = False
    central_daq_address: str | None = None
    influxdb_url: str | None = None
    influxdb_token: str | None = None
    influxdb_org: str | None = None
    influxdb_bucket: str | None = None
    influxdb_measurement_prefix: str | None = None
