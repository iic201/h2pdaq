# H2PControl DAQ Example

This example shows the common producer-server pattern:

1. Create a `LocalDAQ`.
2. Keep lightweight live data in a `PreviewBuffer`.
3. Expose that preview data through `GuiService`.
4. Commit selected previews to DAQ with tags and metadata.
5. Optionally capture control RPC calls with the `@capture` decorator.
6. Configure local InfluxDB writes.

The code below is intentionally small. In a real instrument server, replace `read_sensor()`
with your hardware call and register your own instrument service next to `GuiService`.

## Configure Local DAQ

```python
from h2pcontrol_daq import DAQConfig, LocalDAQ

daq = LocalDAQ(
    DAQConfig(
        enable_local_influx=True,
        influxdb_url="http://localhost:8086",
        influxdb_token="YOUR_TOKEN",
        influxdb_org="beyerlab",
        influxdb_bucket="h2pcontrol",
        influxdb_measurement_prefix="example",
    )
)
```

With `enable_local_influx=True`, each committed event is written to InfluxDB as:

- measurement: `<measurement_prefix>_<source>`
- tags: `run_id`, `producer_id`, `source`, `method`, `direction`, plus event-level `tags`
- fields: finite numeric and boolean values from event data

Keep tags small and stable. Good tags are labels such as `scan_id`, `rid`,
`experiment`, `source`, `device`, and `sweep_axis`. Put large configs, notes,
arrays, and analysis results in metadata or data.

## Preview Buffer And Frames

`PreviewBuffer` stores recent live frames in memory. The GUI can poll or stream these
frames without committing everything to disk.

```python
from h2pcontrol_daq.buffer import PreviewBuffer, PreviewFrame

PRODUCER_ID = "example-producer"
PREVIEW_SOURCE = "temperature"

preview_buffer = PreviewBuffer(max_history=200)
latest_preview: PreviewFrame | None = None


def update_preview(value_celsius: float) -> PreviewFrame:
    global latest_preview
    latest_preview = preview_buffer.update(
        source=PREVIEW_SOURCE,
        producer_id=PRODUCER_ID,
        data={"temperature_celsius": value_celsius},
        metadata={"unit": "degC", "kind": "scalar"},
    )
    return latest_preview
```

A `PreviewFrame` contains:

```text
source       # logical data stream, such as "temperature" or "picoscope"
producer_id  # instrument/server identity
timestamp    # ISO timestamp assigned by PreviewBuffer
data         # your lightweight preview payload
metadata     # descriptive metadata
sequence_id  # per-source monotonically increasing preview counter
```

## Implement GuiService

`GuiService` turns your preview frames into GUI `Frame` messages. This example serves
a scalar stream and supports saving a selected interval to `LocalDAQ`.

```python
from __future__ import annotations

import asyncio
from datetime import datetime
from typing import AsyncIterator

import grpc
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct
from google.protobuf.timestamp_pb2 import Timestamp

from h2pcontrol.sdk import H2PServer, H2PServerConfig
from h2pcontrol.gui.v1.gui_service_pb2 import (
    FRAME_KIND_SCALAR,
    Frame,
    FramePayload,
    GetInfoRequest,
    GetInfoResponse,
    GetLatestRequest,
    GetLatestResponse,
    SaveIntervalRequest,
    SaveIntervalResponse,
    SavedFrame,
    ScalarPayload,
    StartRequest,
    StartResponse,
    StopRequest,
    StopResponse,
    StreamFramesRequest,
    StreamFramesResponse,
)
from h2pcontrol.gui.v1.gui_service_pb2_grpc import (
    GuiServiceServicer,
    add_GuiServiceServicer_to_server,
)
from h2pcontrol_daq import DAQConfig, LocalDAQ
from h2pcontrol_daq.buffer import PreviewBuffer, PreviewFrame


PRODUCER_ID = "example-temperature-monitor"
PREVIEW_SOURCE = "temperature"

daq = LocalDAQ(
    DAQConfig(
        enable_local_influx=True,
        influxdb_url="http://localhost:8086",
        influxdb_token="YOUR_TOKEN",
        influxdb_org="beyerlab",
        influxdb_bucket="h2pcontrol",
        influxdb_measurement_prefix="example",
    )
)
preview_buffer = PreviewBuffer(max_history=200)


def timestamp_now() -> Timestamp:
    timestamp = Timestamp()
    timestamp.GetCurrentTime()
    return timestamp


def context_active(context) -> bool:
    is_active = getattr(context, "is_active", None)
    if callable(is_active):
        return bool(is_active())
    cancelled = getattr(context, "cancelled", None)
    if callable(cancelled):
        return not cancelled()
    return True


def read_sensor() -> float:
    # Replace this with a real hardware read.
    return 21.5


class ExampleServer(H2PServer, GuiServiceServicer):
    def __init__(self, config: H2PServerConfig) -> None:
        super().__init__(config)
        self._latest_preview: PreviewFrame | None = None
        self._preview_running = True
        self._lock = asyncio.Lock()

    def _healthy(self) -> bool:
        return True

    def _add_to_server(self, server: grpc.aio.Server) -> None:
        add_GuiServiceServicer_to_server(self, server)

    def _update_preview_locked(self) -> PreviewFrame:
        value = read_sensor()
        self._latest_preview = preview_buffer.update(
            source=PREVIEW_SOURCE,
            producer_id=PRODUCER_ID,
            data={"temperature_celsius": value},
            metadata={"unit": "degC", "kind": "scalar"},
        )
        return self._latest_preview

    def _preview_to_frame(self, preview: PreviewFrame) -> Frame:
        observed_at = Timestamp()
        observed_at.FromDatetime(datetime.fromisoformat(preview.timestamp))

        metadata = Struct()
        metadata.update({**preview.metadata, "preview": preview.data})

        return Frame(
            source=preview.source,
            producer_id=preview.producer_id,
            sequence_id=preview.sequence_id,
            observed_at=observed_at,
            kind=FRAME_KIND_SCALAR,
            payload=FramePayload(
                scalar=ScalarPayload(
                    value=float(preview.data["temperature_celsius"]),
                    unit="degC",
                )
            ),
            metadata=metadata,
        )

    async def GetInfo(self, request: GetInfoRequest, context) -> GetInfoResponse:
        _ = request, context
        metadata = Struct()
        metadata.update({"example": "temperature monitor"})
        return GetInfoResponse(
            instrument_id=PRODUCER_ID,
            display_name="Example Temperature Monitor",
            service_name=self._config.service.name,
            sources=[PREVIEW_SOURCE],
            metadata=metadata,
        )

    async def Start(self, request: StartRequest, context) -> StartResponse:
        _ = request, context
        self._preview_running = True
        return StartResponse(running=True)

    async def Stop(self, request: StopRequest, context) -> StopResponse:
        _ = request, context
        self._preview_running = False
        return StopResponse(running=False)

    async def GetLatest(self, request: GetLatestRequest, context) -> GetLatestResponse:
        source = request.source or PREVIEW_SOURCE
        async with self._lock:
            preview = preview_buffer.latest(source)
            if preview is None or preview.sequence_id < request.min_sequence_id:
                preview = self._update_preview_locked()

        if preview.source != source or preview.sequence_id < request.min_sequence_id:
            return GetLatestResponse(has_frame=False)

        return GetLatestResponse(frame=self._preview_to_frame(preview), has_frame=True)

    async def StreamFrames(
        self,
        request: StreamFramesRequest,
        context,
    ) -> AsyncIterator[StreamFramesResponse]:
        source = request.source or PREVIEW_SOURCE
        interval = float(request.interval_seconds or 0.5)
        last_sequence_id: int | None = None

        while context_active(context):
            if self._preview_running:
                async with self._lock:
                    preview = self._update_preview_locked()

                if preview.source == source and (
                    not request.emit_on_change_only
                    or preview.sequence_id != last_sequence_id
                ):
                    last_sequence_id = preview.sequence_id
                    yield StreamFramesResponse(frame=self._preview_to_frame(preview))

            await asyncio.sleep(max(interval, 0.05))

    async def SaveInterval(
        self,
        request: SaveIntervalRequest,
        context,
    ) -> SaveIntervalResponse:
        source = request.source or PREVIEW_SOURCE
        start_dt = request.start_observed_at.ToDatetime()
        end_dt = request.end_observed_at.ToDatetime()

        previews = [
            preview
            for preview in preview_buffer.history(source)
            if start_dt <= datetime.fromisoformat(preview.timestamp) <= end_dt
            and (not request.start_sequence_id or preview.sequence_id >= request.start_sequence_id)
            and (not request.end_sequence_id or preview.sequence_id <= request.end_sequence_id)
        ]
        if not previews:
            await context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"No previews for source {source!r} in selected interval",
            )

        analysis = MessageToDict(request.analysis, preserving_proto_field_name=True)
        user_metadata = MessageToDict(
            request.user_metadata,
            preserving_proto_field_name=True,
        )
        tags = MessageToDict(request.tags, preserving_proto_field_name=True)

        saved_frames: list[SavedFrame] = []
        for preview in previews:
            event_id = daq.commit_preview(
                preview,
                method=request.method or "manual_interval",
                analysis=analysis,
                user_metadata=user_metadata,
                tags=tags,
            )

            observed_at = Timestamp()
            observed_at.FromDatetime(datetime.fromisoformat(preview.timestamp))
            saved_frames.append(
                SavedFrame(
                    event_id=event_id,
                    sequence_id=preview.sequence_id,
                    observed_at=observed_at,
                )
            )

        return SaveIntervalResponse(
            frames=saved_frames,
            saved_count=len(saved_frames),
            first_sequence_id=saved_frames[0].sequence_id,
            last_sequence_id=saved_frames[-1].sequence_id,
        )
```

## Start And Stop DAQ With The Server

Start the DAQ pipeline before serving requests. Stop it during shutdown so queued CSV,
HDF5, central-stream, and Influx writes are flushed.

```python
async def main() -> None:
    cfg = H2PServerConfig.load()
    service = ExampleServer(cfg)
    await asyncio.gather(service.start(), daq.start())


if __name__ == "__main__":
    asyncio.run(main())
```

If your server framework already manages lifecycle hooks, call `await daq.start()` in
startup and `await daq.stop()` in shutdown.

## Use Tags

Tags are event-level labels. They are written to local CSV/HDF5 and become InfluxDB
tags, which makes them useful for querying and grouping.

```python
event_id = daq.commit(
    source="picoscope",
    method="manual_capture",
    data={
        "peak_voltage": 0.83,
        "pulse_area": 1.7e-6,
    },
    metadata={
        "sample_rate_hz": 125_000_000,
        "channels": ["A"],
    },
    tags={
        "experiment": "bfield_scan",
        "scan_id": "scan_001",
        "rid": "123456",
        "device": "picoscope_5444d",
        "sweep_axis": "coil_current",
    },
)
```

Nested tag dictionaries are flattened:

```python
tags={
    "scan": {"id": "scan_001", "axis": "coil_current"},
}
```

becomes:

```text
scan.id = scan_001
scan.axis = coil_current
```

Use tags for stable labels. Avoid high-cardinality or bulky values such as full JSON
configs, waveform arrays, long notes, or timestamps. Put those in `metadata` or `data`.

## Use The Decorator

The `@capture` decorator logs async method calls. It can capture inbound args/kwargs,
outbound results, or both.

```python
from h2pcontrol_daq import capture


class InstrumentService:
    @capture(
        daq,
        source="temperature",
        direction="both",
        in_kwargs=["samples"],
        tags={
            "experiment": "checkout",
            "device": "temperature_sensor",
        },
    )
    async def Read(self, request, context):
        value = read_sensor()
        return {"temperature_celsius": value}
```

Important details:

- The decorator currently targets async methods.
- `direction="in"` stores selected args/kwargs before the method runs.
- `direction="out"` stores the return value after the method completes.
- `direction="both"` stores both.
- Static `tags` passed to `@capture` are attached to every captured event from that method.

## Commit A Preview Manually

If you already have a `PreviewFrame`, commit it directly:

```python
preview = preview_buffer.update(
    source="temperature",
    producer_id=PRODUCER_ID,
    data={"temperature_celsius": 21.5},
    metadata={"unit": "degC"},
)

event_id = daq.commit_preview(
    preview,
    method="manual_commit",
    analysis={"mean_celsius": 21.5},
    user_metadata={"operator": "lab_user"},
    tags={
        "experiment": "stability_test",
        "scan_id": "scan_002",
    },
)
```

`commit_preview` stores:

- `data.preview`: the preview payload
- `data.analysis`: optional analysis results
- `data.metadata`: preview metadata plus user metadata
- event-level `tags`: query labels for files and InfluxDB

## Vector Frames

For vector data, use `FRAME_KIND_VECTOR` and `VectorPayload`:

```python
from h2pcontrol.gui.v1.gui_service_pb2 import (
    FRAME_KIND_VECTOR,
    Channel,
    FramePayload,
    VectorPayload,
)

frame.kind = FRAME_KIND_VECTOR
frame.channels.extend([
    Channel(name="x", unit="G"),
    Channel(name="y", unit="G"),
    Channel(name="z", unit="G"),
])
frame.payload.CopyFrom(
    FramePayload(
        vector=VectorPayload(
            values=[0.1, 0.2, 0.3],
            units=["G", "G", "G"],
            names=["x", "y", "z"],
        )
    )
)
```

Your magnetic-field server already follows this pattern.

## Waveform Frames

For waveform-like data, use `FRAME_KIND_WAVEFORM` with an array payload. Keep GUI
previews decimated if the raw waveform is large, and commit the full-resolution data
through DAQ when needed.

```python
import numpy as np

from h2pcontrol.gui.v1.gui_service_pb2 import (
    ARRAY_ENCODING_RAW_F64_LE,
    FRAME_KIND_WAVEFORM,
    ArrayPayload,
    Axis,
    Channel,
    FramePayload,
)

time_s = np.linspace(0.0, 1.0e-3, 1000, dtype="<f8")
voltage_v = np.sin(2.0 * np.pi * 10_000.0 * time_s).astype("<f8")

frame.kind = FRAME_KIND_WAVEFORM
frame.axes.extend([Axis(name="time", unit="s", values=time_s.tolist())])
frame.channels.extend([Channel(name="A", unit="V")])
frame.payload.CopyFrom(
    FramePayload(
        array=ArrayPayload(
            data=voltage_v.tobytes(),
            shape=list(voltage_v.shape),
            dtype="float64",
            encoding=ARRAY_ENCODING_RAW_F64_LE,
        )
    )
)
```

For a PicoScope, a good split is:

- GUI preview: decimated waveform plus channel/time metadata
- DAQ commit: full waveform, analysis scalars, metadata, and tags such as `rid`,
  `scan_id`, `sweep_axis`, and `device`

