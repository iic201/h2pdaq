## Local DAQ

The local DAQ package is the producer-side capture layer for H2PControl instrument servers. Its purpose is to turn method calls and GUI preview commits into structured `DAQEvent`s, persist them locally, and optionally stream them to a central DAQ service.

```mermaid
flowchart TB
    A[Producer service] --> B[capture(...) decorator]
    B --> C[PendingEvent]
    C --> D[LocalDAQ ingress queue]
    D --> E[Serializer task]
    E --> F[DAQEvent]
    F --> G[JSONL writer]
    F --> H[CSV writer]
    F --> I[HDF5 writer]
    F -. optional .-> J[Central DAQ gRPC sink]
    F -. optional .-> P[Local InfluxDB writer]

    G --> K["data/jsonl/daq_capture_<run_id>.jsonl"]
    H --> L["data/csv/daq_capture_<run_id>.csv"]
    I --> M["data/hdf5/daq_capture_<run_id>.hdf5"]
    P --> Q["InfluxDB bucket"]

    E -. stats .-> N[LocalDAQStats]
    G -. logs .-> O[.logs/daq-info.log]
    H -. logs .-> O
    I -. logs .-> O
```

### Current Flow

1. A producer creates a `LocalDAQ` instance.
2. The producer starts it with `await daq.start()`.
3. Async methods can be decorated with `capture(...)`.
4. The decorator creates a `PendingEvent` before the wrapped method runs when `direction` includes `in`.
5. The decorator creates another `PendingEvent` after the method returns when `direction` includes `out`.
6. `LocalDAQ.publish_pending_event(...)` assigns a monotonically increasing event id.
7. The serializer task converts each `PendingEvent` into a `DAQEvent`.
8. The event is fanned out to the configured CSV, HDF5, and/or local InfluxDB writer queues, and optionally to central gRPC.
9. `await daq.stop()` flushes queues and cancels writer tasks during shutdown.

Manual preview commits use the same pipeline through:

```python
daq.commit_preview(preview, method="manual_interval")
```

This is what GUI `SaveInterval` handlers call in instrument servers.

### Run IDs

The package generates one `run_id` per Python process. Decorated captures and manual commits use this generated run id unless a caller explicitly passes a different one.

That means files are grouped by run:

```text
data/jsonl/daq_capture_<run_id>.jsonl
data/csv/daq_capture_<run_id>.csv
data/hdf5/daq_capture_<run_id>.hdf5
```

### What It Stores

Each event contains:

- `event_id`
- `timestamp`
- `run_id`
- `producer_id`
- `source`
- `method`
- `direction`
- `data`
- `tags`

For decorated methods, inbound events contain selected args/kwargs and outbound events contain the returned result.

For preview commits, event data contains:

- `preview`
- `analysis`
- optional `metadata`

Preview tags are also mirrored into the event-level `tags` map so file writers, central streaming, and InfluxDB can treat them as query labels.

### Save Formats

By default, `LocalDAQ` writes CSV and HDF5:

```python
from h2pcontrol_daq import DAQConfig, DAQSaveFormat, LocalDAQ

daq = LocalDAQ(
    DAQConfig(
        save_formats=(DAQSaveFormat.CSV, DAQSaveFormat.HDF5),
    )
)
```

Supported values are `DAQSaveFormat.CSV`, `DAQSaveFormat.HDF5`, and
`DAQSaveFormat.INFLUX`. String values such as `"csv"`, `"hdf5"`, and `"influx"` are
also accepted.

Manual commits and decorators can override the config for a specific event:

```python
daq.commit(
    source="counter",
    method="manual_capture",
    data={"value": 42},
    save_formats=("csv",),
)

@capture(daq, source="counter", save_formats=("hdf5", "influx"))
async def Read(self, request, context):
    ...
```

`None` means "use `DAQConfig.save_formats`"; an empty tuple means "do not write this
event to any local save format."

### File Formats

JSONL keeps the event shape directly:

```json
{"event_id": 1, "run_id": "...", "source": "counter", "data": {...}}
```

CSV is tabular. Common event fields are normal columns, and nested `data` fields are flattened:

```text
event_id,timestamp,run_id,producer_id,source,method,direction,data.preview.state.value
```

HDF5 is hierarchical. Each event is stored under:

```text
/<run_id>/<event_id>/
```

Event metadata is stored as HDF5 attributes. Nested event data is stored as HDF5 groups and datasets under `data`.

### Runtime Behavior

The implementation is asynchronous and queue-based. The wrapped producer method does not normally block on file I/O because writers run in background tasks.

Runtime counters track:

- published events
- serialized events
- dropped ingress events
- dropped outbound CSV events
- dropped outbound HDF5 events
- dropped outbound JSONL events
- dropped outbound central-stream events
- dropped outbound InfluxDB events
- serialization errors

Queue overflow is controlled by `OverflowPolicy`:

- `DROP_NEWEST`
- `DROP_OLDEST`
- `BLOCK_WITH_TIMEOUT`

### Central Streaming

`LocalDAQ` creates a `GrpcDAQSink` for optional central streaming. By default it targets:

```text
127.0.0.1:50052
```

Central streaming is controlled by `DAQConfig.enable_central_stream`.

### Local InfluxDB

Local InfluxDB writes are controlled by `DAQConfig.save_formats` and the legacy
`DAQConfig.enable_local_influx` flag. When `DAQSaveFormat.INFLUX` is selected, the
event is written as an Influx point. When `enable_local_influx=True`, every committed
event uses Influx unless a per-event `save_formats` override is provided.

```python
from h2pcontrol_daq import DAQConfig, LocalDAQ

daq = LocalDAQ(
    DAQConfig(
        save_formats=("csv", "hdf5", "influx"),
        influxdb_url="http://localhost:8086",
        influxdb_token="...",
        influxdb_org="beyer-labs",
        influxdb_bucket="h2pcontrol",
    )
)
```

Configuration can also come from:

```text
INFLUXDB_URL
INFLUXDB_TOKEN or INFLUXDB_ADMIN_TOKEN
INFLUXDB_ORG
INFLUXDB_BUCKET
INFLUXDB_MEASUREMENT_PREFIX
```

The writer uses `<measurement_prefix>_<source>` as the measurement name. It stores `run_id`, `producer_id`, `source`, `method`, `direction`, and event-level `tags` as InfluxDB tags. It stores only finite numeric and boolean measurement values as fields. Bulky descriptive subtrees such as `analysis`, `metadata`, and board/static info stay in JSONL/CSV/HDF5 instead of being expanded into InfluxDB fields.

### Notes

- The decorator currently targets async methods.
- Synchronous methods would need a separate wrapper.
- The local files are written relative to the current working directory of the server process.
- The local DAQ package can be used without running the central DAQ server.
