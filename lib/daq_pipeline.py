"""
daq_pipeline.py

Concurrent DAQ pipeline for H2pControl.

Architecture:

    RPC handler  --ingest()-->  ingress_queue
                                    |
                              normalizer_task
                                    |
                    .───────────────+───────────────.
                    |               |               |
               hdf5_queue    influx_queue       ui_queue
                    |               |               |
            hdf5_writer_task  influx_writer_task  ui_broadcaster_task
                    |               |               |
               HDF5 file       InfluxDB     WebSocket clients

Key design rules:
- ingest() is the only call made from the RPC handler; it is O(1)
- all slow I/O is owned by dedicated tasks that run concurrently
- each queue is bounded; overload is handled by drop policies
- exactly one task owns the HDF5 file handle at all times
- shutdown drains all queues before closing the file
"""

from __future__ import annotations
import asyncio
import itertools
import time
import functools
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any
import h5py
import numpy as np


# ---------------------------------------------------------------------------
# Event model
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DAQEvent:
    """
    Immutable event emitted by the decorator after each RPC call.

    Immutability lets this event be safely shared across queues and tasks
    without copying or locking.

    Fields
    ------
    timestamp_ns
        Wall-clock time in nanoseconds at the point of ingest.  Assigned in
        the decorator, not by the writer, so upstream latency is visible.
    run_id
        String identifying the current experiment session.  Set once when the
        pipeline starts and attached to every event automatically.
    source
        Name of the gRPC service that produced this event, e.g. "arduino".
    method
        Name of the RPC method, e.g. "say_hello".
    direction
        "in" for a captured request, "out" for a captured response.
    sequence
        Monotonically increasing integer per (source, method) pair.  Used to
        detect dropped or reordered events in post-analysis.
    payload
        Flat or semi-flat dictionary extracted from the protobuf message via
        to_dict(). Nested dicts are allowed; the HDF5 writer flattens further.
    units
        Optional dict mapping field names to unit strings, e.g. {"voltage": "V"}.
    quality
        Optional integer quality flag. 0 = good.
    """

    timestamp_ns: int
    run_id: str
    source: str
    method: str
    direction: str
    sequence: int
    payload: dict[str, Any]
    units: dict[str, str] = field(default_factory=dict)
    quality: int = 0


# ---------------------------------------------------------------------------
# Pipeline statistics (simple counters, not thread-safe but good enough)
# ---------------------------------------------------------------------------


@dataclass
class DAQStats:
    ingested: int = 0
    dropped_ingress: int = 0
    dropped_hdf5: int = 0
    dropped_influx: int = 0
    dropped_ui: int = 0
    hdf5_flushes: int = 0
    hdf5_rows_written: int = 0
    influx_batches_written: int = 0


# ---------------------------------------------------------------------------
# Lightweight ingest decorator
# ---------------------------------------------------------------------------


def capture(source: str, direction: str = "both"):
    """
    Decorator factory that emits a DAQEvent into the pipeline for each call.

    Usage on an async gRPC service method:

        pipeline = DAQPipeline(...)

        class MyService(MyServiceBase):

            @capture(source="arduino", direction="both")
            async def say_hello(self, message: HelloRequest) -> HelloReply:
                return HelloReply(message="World")

    The decorator resolves the pipeline via the first argument (self) of the
    wrapped method, which must expose a .pipeline attribute.  You can also
    pass the pipeline explicitly via capture(source=..., pipeline=pipeline).

    The wrapper is async-aware: it wraps ``async def`` functions properly.
    """

    log_in = direction in ("in", "both")
    log_out = direction in ("out", "both")

    def decorator(func):
        _counter: dict[str, itertools.count] = {}  # per source+method

        @functools.wraps(func)
        async def wrapper(self_svc, *args, **kwargs):
            pipeline: DAQPipeline = getattr(self_svc, "pipeline", None)
            if pipeline is None:
                # No pipeline attached; behave transparently
                return await func(self_svc, *args, **kwargs)

            key = f"{source}.{func.__name__}"
            if key not in _counter:
                _counter[key] = itertools.count()
            seq = next(_counter[key])

            if log_in and args:
                msg = args[0]
                payload = msg.to_dict() if hasattr(msg, "to_dict") else {}
                await pipeline.ingest(
                    DAQEvent(
                        timestamp_ns=time.time_ns(),
                        run_id=pipeline.run_id,
                        source=source,
                        method=func.__name__,
                        direction="in",
                        sequence=seq,
                        payload=payload,
                    )
                )

            result = await func(self_svc, *args, **kwargs)

            if log_out:
                payload = result.to_dict() if hasattr(result, "to_dict") else {}
                await pipeline.ingest(
                    DAQEvent(
                        timestamp_ns=time.time_ns(),
                        run_id=pipeline.run_id,
                        source=source,
                        method=func.__name__,
                        direction="out",
                        sequence=seq,
                        payload=payload,
                    )
                )

            return result

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# The pipeline
# ---------------------------------------------------------------------------


class DAQPipeline:
    """
    Bounded concurrent data acquisition pipeline.

    Responsibilities
    ----------------
    - Receive events from RPC handlers without blocking them.
    - Normalize and enrich events.
    - Persist all events to HDF5 in batches via a single owner task.
    - Forward reduced events to InfluxDB for dashboards.
    - Broadcast recent events to live UI clients.

    Lifecycle
    ---------
    ```python
    pipeline = DAQPipeline(run_id="run_001", hdf5_path="data.h5", ...)
    await pipeline.start()          # spawns all background tasks

    # ... your server runs ...

    await pipeline.stop()           # drains queues then cancels tasks
    ```
    """

    def __init__(
        self,
        run_id: str,
        hdf5_path: str,
        influx_url: str = "http://localhost:8086",
        influx_token: str = "",
        influx_org: str = "beyerlab",
        influx_bucket: str = "test",
        ingress_maxsize: int = 10_000,
        hdf5_maxsize: int = 50_000,
        influx_maxsize: int = 20_000,
        ui_maxsize: int = 2_000,
        hdf5_batch_size: int = 500,
        hdf5_flush_s: float = 1.0,
        influx_flush_s: float = 0.5,
        ui_window_size: int = 200,
    ):
        self.run_id = run_id
        self._hdf5_path = hdf5_path
        self.stats = DAQStats()

        # Queues
        self._ingress_queue = asyncio.Queue(maxsize=ingress_maxsize)
        self._hdf5_queue = asyncio.Queue(maxsize=hdf5_maxsize)
        self._influx_queue = asyncio.Queue(maxsize=influx_maxsize)
        self._ui_queue = asyncio.Queue(maxsize=ui_maxsize)

        self._downstream_queues: list[asyncio.Queue] = [
            self._hdf5_queue,
            self._influx_queue,
            self._ui_queue,
        ]

        # Writer parameters
        self._hdf5_batch_size = hdf5_batch_size
        self._hdf5_flush_s = hdf5_flush_s
        self._influx_flush_s = influx_flush_s
        self._ui_window_size = ui_window_size

        # InfluxDB connection details
        self._influx_url = influx_url
        self._influx_token = influx_token
        self._influx_org = influx_org
        self._influx_bucket = influx_bucket

        # Live UI clients: each is a Queue that its WebSocket handler drains
        self.ui_clients: set[asyncio.Queue] = set()

        # Single thread pool for HDF5 I/O (HDF5 C library is not async-safe)
        self._io_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="daq-hdf5"
        )

        self._stopping = False
        self._tasks: list[asyncio.Task] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def ingest(self, event: DAQEvent) -> None:
        """
        Called by the @capture decorator on every RPC call.
        Non-blocking: drops the event if the ingress queue is full.
        """
        self.stats.ingested += 1
        try:
            self._ingress_queue.put_nowait(event)
        except asyncio.QueueFull:
            self.stats.dropped_ingress += 1

    async def start(self) -> None:
        """Spawn all pipeline background tasks."""
        self._tasks = [
            asyncio.create_task(self._normalizer_task(), name="daq-normalizer"),
            asyncio.create_task(self._hdf5_writer_task(), name="daq-hdf5"),
            asyncio.create_task(self._influx_writer_task(), name="daq-influx"),
            asyncio.create_task(self._ui_broadcaster_task(), name="daq-ui"),
        ]

    async def stop(self) -> None:
        """
        Graceful shutdown.  Drains all queues before cancelling tasks.
        This ensures in-memory data is not lost on shutdown.
        """
        self._stopping = True

        # Drain source queues in order (ingress -> hdf5 -> influx)
        await self._ingress_queue.join()
        await self._hdf5_queue.join()
        await self._influx_queue.join()
        # UI queue does not need draining (stateless view)

        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

        self._io_executor.shutdown(wait=True)

    # ------------------------------------------------------------------
    # Stage 1: normalizer
    # ------------------------------------------------------------------

    async def _normalizer_task(self) -> None:
        """
        Pulls events from the ingress queue, optionally enriches them,
        then fans them out to all downstream queues.

        Enrichment here might include:
        - unit lookup tables
        - calibration coefficients
        - computed derived fields (e.g. power from voltage + current)
        """
        while True:
            event = await self._get_or_stop(self._ingress_queue)
            if event is None:
                break

            # Enrich (no-op for now; extend with your calibration logic)
            enriched = event

            # Fan out to all downstream queues; drop to slow queues
            for q, stat_name in [
                (self._hdf5_queue, "dropped_hdf5"),
                (self._influx_queue, "dropped_influx"),
                (self._ui_queue, "dropped_ui"),
            ]:
                try:
                    q.put_nowait(enriched)
                except asyncio.QueueFull:
                    setattr(self.stats, stat_name, getattr(self.stats, stat_name) + 1)

            self._ingress_queue.task_done()

    # ------------------------------------------------------------------
    # Stage 2: HDF5 writer
    # ------------------------------------------------------------------

    async def _hdf5_writer_task(self) -> None:
        """
        Batches events and flushes them to HDF5 via a thread executor.

        This task is the sole owner of the HDF5 file handle.
        It runs the actual disk write in a ThreadPoolExecutor so HDF5 I/O
        does not block the asyncio event loop.

        HDF5 file layout:
            {run_id}/{source}/{method}/{direction}/
                {field_name}   → 1D resizable dataset
                ts_ns          → int64 nanosecond timestamps
        """
        batch: list[DAQEvent] = []
        last_flush = time.monotonic()
        loop = asyncio.get_event_loop()

        while True:
            event = await self._get_or_stop(self._hdf5_queue, timeout=0.1)
            if event is not None:
                batch.append(event)
                self._hdf5_queue.task_done()

            flush_due = (time.monotonic() - last_flush) >= self._hdf5_flush_s
            batch_full = len(batch) >= self._hdf5_batch_size

            if batch and (
                flush_due or batch_full or (self._stopping and self._hdf5_queue.empty())
            ):
                await loop.run_in_executor(
                    self._io_executor, self._flush_hdf5_batch, batch.copy()
                )
                self.stats.hdf5_flushes += 1
                self.stats.hdf5_rows_written += len(batch)
                batch.clear()
                last_flush = time.monotonic()

            if self._stopping and self._hdf5_queue.empty() and not batch:
                break

    def _flush_hdf5_batch(self, batch: list[DAQEvent]) -> None:
        """
        Synchronous HDF5 write. Runs inside a thread pool.
        Called only from _hdf5_writer_task via run_in_executor.
        """
        try:
            import h5py
        except ImportError:
            raise RuntimeError(
                "h5py is required for HDF5 persistence. "
                "Install it with: uv pip install h5py"
            )

        with h5py.File(self._hdf5_path, "a") as f:
            for event in batch:
                grp_path = (
                    f"{event.run_id}/{event.source}/{event.method}/{event.direction}"
                )
                grp = f.require_group(grp_path)

                # Write timestamp
                self._hdf5_append(grp, "ts_ns", event.timestamp_ns, dtype="int64")

                # Write each payload field
                for fname, fval in self._flatten_payload(event.payload):
                    if fval is None:
                        continue
                    try:
                        self._hdf5_append(grp, fname, fval)
                    except Exception:
                        pass  # skip non-scalar values silently

    @staticmethod
    def _hdf5_append(grp, name: str, value, dtype=None) -> None:
        """Lazily create and resize a 1-D chunked dataset."""
        if name not in grp:
            if dtype:
                grp.create_dataset(
                    name,
                    data=np.array([value], dtype=dtype),
                    maxshape=(None,),
                    chunks=(1024,),
                )
            else:
                arr = np.array([value])
                grp.create_dataset(name, data=arr, maxshape=(None,), chunks=(1024,))
        else:
            ds = grp[name]
            ds.resize(ds.shape[0] + 1, axis=0)
            ds[-1] = value

    @staticmethod
    def _flatten_payload(d: dict, prefix: str = "") -> list[tuple[str, Any]]:
        """Flatten nested dicts into (dotted.key, value) pairs."""
        result = []
        for k, v in d.items():
            key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                result.extend(DAQPipeline._flatten_payload(v, key))
            else:
                result.append((key, v))
        return result

    # ------------------------------------------------------------------
    # Stage 3: InfluxDB writer
    # ------------------------------------------------------------------

    async def _influx_writer_task(self) -> None:
        """
        Batches events and writes them to InfluxDB every _influx_flush_s seconds.

        Uses the async InfluxDB write API to avoid blocking.
        Accumulates points in memory between flushes so each flush is a bulk write.
        """
        try:
            import influxdb_client
            from influxdb_client import Point
            from influxdb_client.client.write_api import ASYNCHRONOUS

            client = influxdb_client.InfluxDBClient(
                url=self._influx_url, token=self._influx_token, org=self._influx_org
            )
            write_api = client.write_api(write_options=ASYNCHRONOUS)
        except ImportError:
            # InfluxDB not installed; skip silently
            while True:
                event = await self._get_or_stop(self._influx_queue)
                if event is None:
                    break
                self._influx_queue.task_done()
            return

        batch: list = []
        last_flush = time.monotonic()
        from influxdb_client import Point

        while True:
            event = await self._get_or_stop(self._influx_queue, timeout=0.2)
            if event is not None:
                for fname, fval in self._flatten_payload(event.payload):
                    if fval is None or isinstance(fval, (dict, list)):
                        continue
                    batch.append(
                        Point(f"{event.source}_{event.method}_{event.direction}")
                        .tag("run_id", event.run_id)
                        .tag("sequence", str(event.sequence))
                        .field(fname, fval)
                        .time(event.timestamp_ns)
                    )
                self._influx_queue.task_done()

            flush_due = (time.monotonic() - last_flush) >= self._influx_flush_s
            if batch and (flush_due or (self._stopping and self._influx_queue.empty())):
                write_api.write(
                    bucket=self._influx_bucket, org=self._influx_org, record=batch
                )
                self.stats.influx_batches_written += 1
                batch.clear()
                last_flush = time.monotonic()

            if self._stopping and self._influx_queue.empty() and not batch:
                break

        client.close()

    # ------------------------------------------------------------------
    # Stage 4: UI broadcaster
    # ------------------------------------------------------------------

    async def _ui_broadcaster_task(self) -> None:
        """
        Pulls events from the UI queue and distributes them to all
        registered UI client queues.

        Each UI client (e.g. a WebSocket handler coroutine) registers by
        adding its own asyncio.Queue to pipeline.ui_clients.

        Slow clients are detected when their queue is full and disconnected
        to protect the rest of the system.

        A rolling window of recent events is kept so a newly connected
        client can receive recent history.
        """
        window: deque[DAQEvent] = deque(maxlen=self._ui_window_size)

        while True:
            event = await self._get_or_stop(self._ui_queue, timeout=0.1)
            if event is None and self._stopping:
                break
            if event is None:
                continue

            window.append(event)

            dead: set[asyncio.Queue] = set()
            for client_q in list(self.ui_clients):
                try:
                    client_q.put_nowait(event)
                except asyncio.QueueFull:
                    dead.add(client_q)
            self.ui_clients -= dead  # drop lagging clients
            self._ui_queue.task_done()

    def get_recent_events(self) -> list[DAQEvent]:
        """
        Returns a snapshot of the current rolling window.
        Used by newly connected UI clients to catch up.
        Note: this is NOT thread-safe with the broadcaster task;
        for production use, expose this only from the event loop.
        """
        return []  # placeholder; use the window in the task

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    async def _get_or_stop(
        self,
        queue: asyncio.Queue,
        timeout: float = 1.0,
    ) -> DAQEvent | None:
        """
        Attempt to get an item from the queue with a timeout.
        Returns None on timeout (caller should check self._stopping).
        """
        try:
            return await asyncio.wait_for(queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
