## Local DAQ

`LocalDAQ` is the producer-side capture layer for H2PControl instrument servers. It turns method calls and preview commits into structured `DAQEvent`s, writes selected local outputs, and can stream events to a central DAQ service.

```mermaid
flowchart TB
    A[Producer method] --> B[capture(...) or commit(...)]
    B --> C[PendingEvent]
    C --> D[LocalDAQ ingress queue]
    D --> E[Serializer task]
    E --> F[DAQEvent]
    F --> G[CSV writer]
    F --> H[HDF5 writer]
    F -. if enabled .-> I[Local InfluxDB writer]
    F -. optional .-> J[Central DAQ gRPC sink]
```

## Basic Use

```python
from h2pcontrol_daq import DAQConfig, LocalDAQ, capture

daq = LocalDAQ(DAQConfig(save_formats=("csv",)))

await daq.start()

@capture(daq, source="counter", direction="both")
async def ReadCounter(self, request, context):
    ...

await daq.stop()
```

`start()` creates the serializer and writer tasks. `stop()` waits for queued events, flushes writers, and closes sinks.

## Save Formats

`DAQConfig.save_formats` is the enabled set for this `LocalDAQ` instance. The default is CSV only.

Supported formats:

- `"csv"`
- `"hdf5"`
- `"influx"`

Enum values such as `DAQSaveFormat.CSV` also work.

Per-event overrides on `commit`, `commit_preview`, or `@capture` must be a subset of the configured formats:

```python
daq = LocalDAQ(DAQConfig(save_formats=("csv", "hdf5", "influx")))

daq.commit(
    source="counter",
    method="manual_capture",
    data={"value": 42},
    save_formats=("csv",),
)

@capture(daq, source="field", save_formats=("hdf5", "influx"))
async def ReadField(self, request, context):
    ...
```

Rules:

- `save_formats=None` uses `DAQConfig.save_formats`.
- `save_formats=()` skips local file/Influx writes for that event.
- Asking for a format that was not enabled raises `ValueError`.

## Output Files

Local files are written relative to the current working directory:

```text
data/csv/daq_capture_<run_id>.csv
data/hdf5/daq_capture_<run_id>.hdf5
.logs/daq-info.log
.logs/daq-error.log
```

One `run_id` is generated per Python process unless the caller passes a custom one.

## Local InfluxDB

Enable InfluxDB by including `"influx"` in `DAQConfig.save_formats`:

```python
daq = LocalDAQ(
    DAQConfig(
        save_formats=("csv", "influx"),
        influxdb_url="http://localhost:8086",
        influxdb_token="...",
        influxdb_org="beyer-labs",
        influxdb_bucket="h2pcontrol",
    )
)
```

The same settings can come from environment variables:

```text
INFLUXDB_URL
INFLUXDB_TOKEN or INFLUXDB_ADMIN_TOKEN
INFLUXDB_ORG
INFLUXDB_BUCKET
INFLUXDB_MEASUREMENT_PREFIX
```

Influx measurements use `<measurement_prefix>_<source>`, for example `daq_counter`. Base event fields and event `tags` become tags. Finite numeric and boolean values become fields.

## Capture And Commit

`capture(...)` supports sync and async methods. It can create input events, output events, or both:

```python
@capture(daq, source="counter", direction="both")
def read_counter(self):
    return 42
```

Manual commits use the same pipeline:

```python
daq.commit(source="counter", method="manual", data={"value": 42})
daq.commit_preview(preview, method="manual_interval")
```

## Runtime Behavior

The pipeline is asynchronous and queue-based, so producer methods normally do not block on file or Influx writes.

Stats track published events, serialized events, dropped queues, dropped Influx writes, and serialization errors.

Queue overflow is controlled by `OverflowPolicy`:

- `DROP_NEWEST`
- `DROP_OLDEST`
- `BLOCK_WITH_TIMEOUT`

Central streaming is optional and controlled by `DAQConfig.enable_central_stream`.
