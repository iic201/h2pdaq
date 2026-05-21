from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal

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
    
@dataclass(slots=True)
class CentralDAQStats:
    received: int = 0
    ingested: int = 0
    dropped_ingress: int = 0
    dropped_outbound_csv: int = 0
    dropped_outbound_hdf5: int = 0
    dropped_outbound_jsonl: int = 0
    dropped_outbound_influx: int = 0
    dropped_outbound_central: int = 0
    serialization_errors: int = 0

class OverflowPolicy(StrEnum):
    DROP_NEWEST = "drop_newest"
    DROP_OLDEST = "drop_oldest"
    BLOCK_WITH_TIMEOUT = "block_with_timeout"

@dataclass(slots=True)
class CentralDAQConfig:
    ingest_maxsize: int = 100_000 # Maximum number of PendingEvent in the ingress queue before applying overflow policy.
    outbound_maxsize: int = 100_000 # Maximum number of serialized events in the outbound queue before applying overflow policy.
    queue_put_timeout_s: float = 0.1 # Timeout in seconds for blocking a put operation to the queue when using BLOCK_WITH_TIMEOUT policy.
    ingress_overflow: OverflowPolicy = OverflowPolicy.DROP_NEWEST
    outbound_overflow: OverflowPolicy = OverflowPolicy.DROP_NEWEST
    address: str | None = None

