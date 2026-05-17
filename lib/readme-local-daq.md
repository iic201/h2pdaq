## Local DAQ

The local DAQ package is the client-side capture layer for the h2pcontrol system. Its purpose is to intercept method calls in a producer service, turn them into structured events, and persist those events locally in a few simple formats.

```mermaid
flowchart TB
	A[Producer service] --> B[capture(...) ]
	B --> C[PendingEvent inbound]
	C --> D[LocalDAQ ingress queue]
	D --> E[Serializer task]
	E --> F[DAQEvent]
	F --> G[JSONL writer]
	F --> H[CSV writer]
	F --> I[HDF5 writer]

	G --> J[data/jsonl/daq_capture_<producer_id>.jsonl]
	H --> K[data/csv/daq_capture_<producer_id>.csv]
	I --> L[data/hdf5/daq_capture_<producer_id>.hdf5]

	E -. publishes metrics .-> M[LocalDAQStats]
	G -. logs .-> N[.logs/daq-info.log]
	H -. logs .-> N
	I -. logs .-> N
```

### Complexity

The current per-event control flow is:

- `capture(...)`: time `O(1)`, space `O(1)` per call, excluding the wrapped producer method.
- `PendingEvent` creation: time `O(1)`, space `O(1)`.
- Ingress queue enqueue: time `O(1)`, space `O(q)` for queued events.
- Serializer task: time `O(1)` per event, space `O(1)`.
- `DAQEvent` creation: time `O(1)`, space `O(1)`.
- JSONL writer: time `O(1)` append per event, space `O(1)`.
- CSV writer: time `O(1)` append per event, space `O(1)`.
- HDF5 writer: time `O(1)` append per event in the current design, space `O(1)`.
- `LocalDAQStats` updates: time `O(1)`, space `O(1)`.

The generated files grow on disk over time, but that file growth is not counted as in-memory working space.

### Current flow

1. A producer decorates an async method with `capture(...)`.
2. The decorator creates a `PendingEvent` before the wrapped method runs when `direction` includes `in`.
3. After the wrapped method returns, it creates another `PendingEvent` when `direction` includes `out`.
4. Each pending event is published into `LocalDAQ`, which assigns a monotonically increasing event id and queues the event for serialization.
5. `LocalDAQ` serializes the event into a `DAQEvent` and fans it out to three writer queues.
6. Separate background writers persist the data to JSONL, CSV, and HDF5 files under the local `data/` folder.

### What it stores

The captured event payload includes:

- `event_id`
- `run_id`
- `producer_id`
- `source`
- `method`
- `direction`
- `message` or `data`

The decorator currently records the selected input arguments and keyword arguments on the inbound event, and JSON-serializes the return value on the outbound event.

### Runtime behavior

The current implementation is asynchronous and queue-based. `LocalDAQ` starts one serializer task and one writer task for each output format, so the wrapped producer method does not block on file I/O during normal operation.

The package also keeps basic runtime counters for:

- published events
- serialized events
- dropped ingress events
- dropped outbound events per format
- serialization errors

Queue overflow is controlled by `OverflowPolicy`, which can drop the newest item, drop the oldest item, or block with a timeout.

### Files written by the current implementation

- `data/jsonl/daq_capture_<producer_id>.jsonl`
- `data/csv/daq_capture_<producer_id>.csv`
- `data/hdf5/daq_capture_<producer_id>.hdf5`
- `.logs/daq-info.log`
- `.logs/daq-error.log`

### Notes on the current state

This is still a local capture pipeline, not a central server. It is useful for validating the producer-side instrumentation, inspecting event shape, and keeping a lightweight on-disk record of captured method calls. The code currently targets async producer methods, so synchronous methods would need an additional wrapper if they should be captured the same way.
