from __future__ import annotations

import io
import math
import struct
import grpc
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable
from google.protobuf.message import DecodeError
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.struct_pb2 import Struct
from google.protobuf.timestamp_pb2 import Timestamp
from h2pcontrol.manager.v1 import manager_pb2
from h2pcontrol.gui.v2 import gui_service_pb2 as pb2
from h2pcontrol.gui.v2 import gui_service_pb2_grpc as pb2_grpc

# GuiService implements the following gRPC API for instrument preview data:
# service GuiService {
#   rpc GetInfo(GetInfoRequest) returns (GetInfoResponse);
#   rpc GetLatest(GetLatestRequest) returns (GetLatestResponse);
#   rpc StreamFrames(StreamFramesRequest) returns (stream StreamFramesResponse);
#   rpc SaveInterval(SaveIntervalRequest) returns (SaveIntervalResponse);
# }

# Gui Simplified plotting unit
# Converts GRPC frames to a flat list of 
# timestamped points with name, value, and unit.
@dataclass(frozen=True, slots=True)
class PreviewPoint:
    timestamp: datetime
    sequence_id: int
    name: str
    value: float
    unit: str = ""


@dataclass(frozen=True, slots=True)
class ArrayPreview:
    values: list[float]
    min_value: float | None
    max_value: float | None
    mean_value: float | None
    count: int

# PreviewInfo contains metadata 
# about the instrument and available data sources.
# Basically the GetInfoResponse turn into a class.
@dataclass(frozen=True, slots=True)
class PreviewInfo:
    instrument_id: str
    display_name: str
    service_name: str
    sources: tuple[str, ...]
    metadata: dict[str, Any]

# ServiceStatus represents one row in the service list table
# it combines the manager health info and 
# the result of probing the GuiService API on that service.
@dataclass(frozen=True, slots=True)
class ServiceStatus:
    name: str
    address: str
    healthy: bool
    gui_connectable: bool
    last_seen: str
    detail: str = ""

# Client Wrapper for the GuiService gRPC API
class GuiServiceClient:
    def __init__(self, target: str) -> None:
        self.target = target
        self.channel = grpc.insecure_channel(target)
        self.stub = pb2_grpc.GuiServiceStub(self.channel)

    # Close the gRPC channel
    def close(self) -> None:
        self.channel.close()

    # GetInfo from the service and convert it to PreviewInfo
    # @param timeout: gRPC call timeout in seconds
    def get_info(self, timeout: float | None = 5.0) -> PreviewInfo:
        response = self.stub.GetInfo(pb2.GetInfoRequest(), timeout=timeout)
        return PreviewInfo(
            instrument_id=response.instrument_id,
            display_name=response.display_name,
            service_name=response.service_name,
            sources=tuple(response.sources),
            metadata=MessageToDict(response.metadata, preserving_proto_field_name=True),
        )

    # Get the latest frame from the service
    # @param timeout: gRPC call timeout in seconds
    def get_latest(self, *, source: str = "", min_sequence_id: int = 0, timeout: float | None = 5.0,):
        response = self.stub.GetLatest(
            pb2.GetLatestRequest(source=source, min_sequence_id=min_sequence_id),
            timeout=timeout,
        )
        if not response.has_frame:
            return None
        return response.frame

    # Stream frames from the service as an iterable generator
    # @param source: The data source to stream
    # @param interval_seconds: Minimum interval between frames in seconds
    # @param emit_on_change_only: If true, only emit frames when data changes
    def stream_frames(self, *, source: str = "", interval_seconds: float = 0.1, emit_on_change_only: bool = True,) -> Iterable[Any]:
        for response in self.open_frame_stream(
            source=source,
            interval_seconds=interval_seconds,
            emit_on_change_only=emit_on_change_only,
        ):
            yield response.frame

    def open_frame_stream(self, *, source: str = "", interval_seconds: float = 0.1, emit_on_change_only: bool = True):
        request = pb2.StreamFramesRequest(
            source=source,
            interval_seconds=interval_seconds,
            emit_on_change_only=emit_on_change_only,
        )
        return self.stub.StreamFrames(request)

    # Save an interval of frames and flow through the pipeline
    # @param source: The data source for the interval
    # @param start_observed_at: Start timestamp of the interval
    # @param end_observed_at: End timestamp of the interval
    # @param start_sequence_id: Optional start sequence ID for the interval
    # @param end_sequence_id: Optional end sequence ID for the interval
    # @param method: Method name or label for how the interval was captured
    # @param analysis: Optional analysis results or metadata to associate with the interval
    # @param user_metadata: Optional user-defined metadata to associate with the interval
    # @param tags: Optional tags to associate with the interval for categorization
    # @param timeout: gRPC call timeout in seconds
    def save_interval(
        self,
        *,
        source: str = "",
        start_observed_at: datetime,
        end_observed_at: datetime,
        start_sequence_id: int = 0,
        end_sequence_id: int = 0,
        method: str = "manual_interval",
        analysis: dict[str, Any] | None = None,
        user_metadata: dict[str, Any] | None = None,
        tags: dict[str, Any] | None = None,
        timeout: float | None = 5.0,
    ):
        response = self.stub.SaveInterval(
            pb2.SaveIntervalRequest(
                source=source,
                start_observed_at=_to_timestamp(start_observed_at),
                end_observed_at=_to_timestamp(end_observed_at),
                start_sequence_id=start_sequence_id,
                end_sequence_id=end_sequence_id,
                method=method,
                analysis=_to_struct(analysis or {}),
                user_metadata=_to_struct(user_metadata or {}),
                tags=_to_struct(tags or {}),
            ),
            timeout=timeout,
        )
        return response


MAX_VECTOR_SERIES = 32
MAX_ARRAY_SERIES = 16
MAX_STRUCT_SERIES = 32

# Convert a gRPC frame to a list of PreviewPoints for plotting
# Input - a frame with scalar, vector, array, or struct_value payload
# Output - a flat list of PreviewPoints with timestamp, name, value, and unit
def frame_to_points(frame) -> list[PreviewPoint]:
    observed_at = frame.observed_at.ToDatetime()
    if observed_at.tzinfo is None: # If the timestamp has no timezone replace witn now UTC time
        observed_at = observed_at.replace(tzinfo=timezone.utc)

    payload_kind = frame.payload.WhichOneof("payload")

    # If payloafd is scalar, extract the single value and return as one PreviewPoint
    if payload_kind == "scalar":
        name = _value_name(frame, 0, frame.source or "value")
        unit = frame.payload.scalar.unit or _value_unit(frame, 0)
        value = _coerce_float(frame.payload.scalar.value)
        if value is None:
            return []
        return [
            PreviewPoint(
                timestamp=observed_at,
                sequence_id=int(frame.sequence_id),
                name=name,
                value=value,
                unit=unit,
            )
        ]

    # If payload is vector, extract each value with its name and unit 
    # and return as a list of PreviewPoints
    if payload_kind == "vector":
        vector = frame.payload.vector
        points = []
        for index, value in enumerate(vector.values[:MAX_VECTOR_SERIES]):
            numeric = _coerce_float(value)
            if numeric is None:
                continue
            name = (
                vector.names[index]
                if index < len(vector.names) and vector.names[index]
                else _value_name(frame, index, f"value_{index}")
            )
            unit = (
                vector.units[index]
                if index < len(vector.units) and vector.units[index]
                else _value_unit(frame, index)
            )
            points.append(_point(frame, observed_at, name, numeric, unit=unit))
        return points

    # If payload is array, extract values up to MAX_ARRAY_SERIES with indexed names,
    # and if there are more values, also include min, max, and mean as additional points.
    if payload_kind == "array":
        preview = _array_preview(frame.payload.array, MAX_ARRAY_SERIES)
        if preview.count <= 0:
            return []
        base_name = _value_name(frame, 0, frame.source or "array")
        unit = _value_unit(frame, 0)
        points = []
        for index, value in enumerate(preview.values):
            name = base_name if preview.count == 1 else f"{base_name}[{index}]"
            points.append(_point(frame, observed_at, name, value, unit=unit))
        if preview.count > len(preview.values):
            if (
                preview.min_value is not None
                and preview.max_value is not None
                and preview.mean_value is not None
            ):
                points.extend(
                    [
                        _point(frame, observed_at, f"{base_name}.min", preview.min_value, unit=unit),
                        _point(frame, observed_at, f"{base_name}.max", preview.max_value, unit=unit),
                        _point(frame, observed_at, f"{base_name}.mean", preview.mean_value, unit=unit),
                    ]
                )
        return points

    # If payload is struct_value, recursively walk the structure to find all numeric values,
    # and return them as PreviewPoints with dot notation names for nested fields.
    if payload_kind == "struct_value":
        data = MessageToDict(frame.payload.struct_value, preserving_proto_field_name=True)
        return [
            _point(frame, observed_at, name, value)
            for name, value in _walk_numeric_values(data, limit=MAX_STRUCT_SERIES)
        ]

    return []

# Convert a gRPC frame to a dictionary
# @param frame: The gRPC frame to convert
def frame_to_dict(frame) -> dict[str, Any]:
    return MessageToDict(frame, preserving_proto_field_name=True)

# Helper function to create a PreviewPoint
# @param frame: The gRPC frame to create the point from
# @param timestamp: The timestamp for the point
# @param name: The name for the point
# @param value: The value for the point
def _point(
    frame,
    timestamp: datetime,
    name: str,
    value: float,
    *,
    unit: str = "",
) -> PreviewPoint:
    return PreviewPoint(
        timestamp=timestamp,
        sequence_id=int(frame.sequence_id),
        name=name,
        value=value,
        unit=unit,
    )

# Helper function to convert a dictionary to a protobuf Struct
# @param data: The dictionary to convert
def _to_struct(data: dict[str, Any]) -> Struct:
    struct = Struct()
    ParseDict(data, struct)
    return struct

# Helper function to convert a datetime to a protobuf Timestamp
# @param value: The datetime to convert
def _to_timestamp(value: datetime) -> Timestamp:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    timestamp = Timestamp()
    timestamp.FromDatetime(value.astimezone(timezone.utc))
    return timestamp

# Helper function to get the name of a value from the frame channels or fallback
# @param frame: The gRPC frame containing the channels
# @param index: The index of the channel to retrieve
# @param fallback: The fallback name if the channel is not found
def _value_name(frame, index: int, fallback: str) -> str:
    if index < len(frame.channels) and frame.channels[index].name:
        return frame.channels[index].name
    return fallback

# Helper function to get the unit of a value from the frame channels or fallback
# @param frame: The gRPC frame containing the channels
# @param index: The index of the channel to retrieve
def _value_unit(frame, index: int) -> str:
    if index < len(frame.channels):
        return frame.channels[index].unit
    return ""

# Helper function to coerce a value to a float if possible, otherwise return None
# @param value: The value to cast to float
def _coerce_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, int | float):
        numeric = float(value)
    elif isinstance(value, str):
        try:
            numeric = float(value.strip())
        except ValueError:
            return None
    else:
        return None
    return numeric if math.isfinite(numeric) else None

# Helper function to recursively walk a nested structure and yield all numeric values with their names
# @param value: The value to walk, which can be a dict, list, or scalar
# @param prefix: The prefix to use for naming the values, which accumulates the
def _walk_numeric_values(
    value: Any,
    prefix: str = "",
    *,
    limit: int | None = None,
) -> Iterable[tuple[str, float]]:
    yielded = 0

    def walk(child_value: Any, child_prefix: str) -> Iterable[tuple[str, float]]:
        nonlocal yielded
        if limit is not None and yielded >= limit:
            return

        numeric = _coerce_float(child_value)
        if numeric is not None:
            yielded += 1
            yield child_prefix or "value", numeric
            return

        if isinstance(child_value, dict):
            for key, grandchild in child_value.items():
                if limit is not None and yielded >= limit:
                    return
                name = str(key)
                grandchild_prefix = f"{child_prefix}.{name}" if child_prefix else name
                yield from walk(grandchild, grandchild_prefix)
            return

        if isinstance(child_value, list | tuple):
            for index, grandchild in enumerate(child_value):
                if limit is not None and yielded >= limit:
                    return
                grandchild_prefix = (
                    f"{child_prefix}[{index}]" if child_prefix else f"value[{index}]"
                )
                yield from walk(grandchild, grandchild_prefix)

    yield from walk(value, prefix)

def _array_preview(array_payload, limit: int) -> ArrayPreview:
    encoding = int(array_payload.encoding)
    data = bytes(array_payload.data)

    if encoding == pb2.ARRAY_ENCODING_RAW_F64_LE:
        return _raw_array_preview(data, "<d", limit)
    if encoding == pb2.ARRAY_ENCODING_RAW_F32_LE:
        return _raw_array_preview(data, "<f", limit)

    preview = _array_preview_with_numpy(data, array_payload.dtype, limit)
    if preview is not None:
        return preview
    return ArrayPreview([], None, None, None, 0)


def _raw_array_preview(data: bytes, fmt: str, limit: int) -> ArrayPreview:
    size = struct.calcsize(fmt)
    usable = len(data) - (len(data) % size)
    if usable <= 0:
        return ArrayPreview([], None, None, None, 0)

    values = []
    min_value = None
    max_value = None
    total = 0.0
    count = 0
    for item in struct.iter_unpack(fmt, data[:usable]):
        numeric = float(item[0])
        if not math.isfinite(numeric):
            continue
        if len(values) < limit:
            values.append(numeric)
        min_value = numeric if min_value is None else min(min_value, numeric)
        max_value = numeric if max_value is None else max(max_value, numeric)
        total += numeric
        count += 1

    mean_value = total / count if count else None
    return ArrayPreview(values, min_value, max_value, mean_value, count)


def _array_preview_with_numpy(data: bytes, dtype: str, limit: int) -> ArrayPreview | None:
    try:
        import numpy as np
    except Exception:
        return None

    try:
        loaded = np.load(io.BytesIO(data), allow_pickle=False)
    except Exception:
        if not dtype:
            return None
        try:
            loaded = np.frombuffer(data, dtype=np.dtype(dtype))
        except Exception:
            return None

    if hasattr(loaded, "files"):
        files = list(loaded.files)
        if not files:
            return ArrayPreview([], None, None, None, 0)
        loaded = loaded[files[0]]

    try:
        values = np.asarray(loaded).reshape(-1).astype(float, copy=False)
    except Exception:
        return ArrayPreview([], None, None, None, 0)
    if values.size == 0:
        return ArrayPreview([], None, None, None, 0)

    finite = values[np.isfinite(values)]
    if finite.size == 0:
        return ArrayPreview([], None, None, None, 0)

    preview_values = [float(value) for value in finite[:limit]]
    return ArrayPreview(
        preview_values,
        float(np.min(finite)),
        float(np.max(finite)),
        float(np.mean(finite)),
        int(finite.size),
    )

# Helper function to format a protobuf Timestamp as an ISO 8601 string, handling None and timezone
# @param value: The protobuf Timestamp to format
def _format_timestamp(value) -> str:
    if value is None:
        return ""
    observed = value.ToDatetime()
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)
    return observed.isoformat(timespec="seconds")

# Function to probe the GuiService API on a given address to check if it's connectable and healthy
# @param address: The address of the service to probe (e.g. "localhost:50051")
# @param timeout: The timeout in seconds for the gRPC call
def _probe_gui_service(address: str, timeout: float = 1.5) -> tuple[bool, str]:
    address = _normalize_address(address)
    if not address:
        return False, "No address registered"
    try:
        client = GuiServiceClient(address)
        try:
            client.get_info(timeout=timeout)
            return True, "GuiService OK"
        finally:
            client.close()
    except grpc.RpcError as exc:
        return False, _format_rpc_error("GuiService", address, exc)
    except Exception as exc:
        return False, str(exc)

# Helper function to normalize an address by stripping whitespace and handling legacy formats
# For example, if the address is in the format "tcp://localhost:50051", it will extract "localhost:50051"
def _normalize_address(address: str) -> str:
    parts = address.strip().split(":")
    if len(parts) >= 3 and parts[-1].isdigit():
        return ":".join(parts[-2:])
    return address.strip()

# Helper function to read a varint from bytes starting at a given offset, returning the value and new offset
# @param data: The bytes to read the varint from
# @param offset: The starting offset in the bytes to read from
def _read_varint(data: bytes, offset: int) -> tuple[int, int]:
    shift = 0
    value = 0
    while offset < len(data):
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, offset
        shift += 7
    raise ValueError("truncated varint")

# Helper function to skip a field in the protobuf binary format based on its wire type, returning the new offset
# @param data: The bytes to skip the field in
# @param offset: The starting offset in the bytes to skip from
# @param wire_type: The protobuf wire type of the field to skip (0=varint, 1=64-bit, 2=length-delimited, 5=32-bit
def _skip_field(data: bytes, offset: int, wire_type: int) -> int:
    if wire_type == 0:
        _, offset = _read_varint(data, offset)
        return offset
    if wire_type == 1:
        return offset + 8
    if wire_type == 2:
        length, offset = _read_varint(data, offset)
        return offset + length
    if wire_type == 5:
        return offset + 4
    raise ValueError(f"unsupported wire type {wire_type}")

# Helper function to decode a legacy server entry from raw bytes, extracting name, description, and address
# @param data: The raw bytes containing the legacy server entry in protobuf binary format
def _decode_legacy_server(data: bytes) -> tuple[str, str, str]:
    offset = 0
    name = ""
    description = ""
    address = ""
    while offset < len(data):
        key, offset = _read_varint(data, offset)
        field_number = key >> 3
        wire_type = key & 0x07
        if wire_type == 2 and field_number in (1, 2, 3):
            length, offset = _read_varint(data, offset)
            text = data[offset:offset + length].decode("utf-8", errors="replace")
            offset += length
            if field_number == 1:
                name = text
            elif field_number == 2:
                description = text
            else:
                address = text
        else:
            offset = _skip_field(data, offset, wire_type)
    return name, description, _normalize_address(address)

# Helper function to decode the legacy FetchServers response, which contains multiple server entries in protobuf binary format
# @param data: The raw bytes containing the legacy FetchServers response with multiple server entries
def _decode_legacy_fetch_servers(data: bytes) -> list[tuple[str, str, str]]:
    offset = 0
    services: list[tuple[str, str, str]] = []
    while offset < len(data):
        key, offset = _read_varint(data, offset)
        field_number = key >> 3
        wire_type = key & 0x07
        if field_number == 1 and wire_type == 2:
            length, offset = _read_varint(data, offset)
            services.append(_decode_legacy_server(data[offset:offset + length]))
            offset += length
        else:
            offset = _skip_field(data, offset, wire_type)
    return services


# Function to get the list of services from the manager using the legacy FetchServers API, with probing for GUI connectivity
# This is used as a fallback if the manager does not support the current List API, 
# allowing compatibility with older manager versions while still checking if the GUI service is available on each registered server.
def _legacy_services(channel: grpc.Channel, timeout: float) -> list[ServiceStatus]:
    fetch_servers = channel.unary_unary(
        "/h2pcontrol.Manager/FetchServers",
        request_serializer=lambda _request: b"",
        response_deserializer=lambda data: data,
    )
    raw_response = fetch_servers(object(), timeout=timeout)
    services: list[ServiceStatus] = []
    for name, description, address in _decode_legacy_fetch_servers(raw_response):
        gui_connectable, detail = _probe_gui_service(address)
        if description:
            detail = f"{detail}; {description}" if detail else description
        services.append(
            ServiceStatus(
                name=name,
                address=address,
                healthy=True,
                gui_connectable=gui_connectable,
                last_seen="",
                detail=detail,
            )
        )
    return sorted(services, key=lambda service: service.name)

# Helper function to create a ServiceStatus object for a service, including probing the GUI service connectivity and combining details
# @param name: The name of the service
# @param address: The address of the service to probe for GUI connectivity
# @param healthy: Whether the service is healthy according to the manager
# @param last_seen: The last seen timestamp for the service, formatted as a string
# @param detail_prefix: An optional prefix to include in the detail field, which will be combined with the GUI probe result
def _service_status(
    *,
    name: str,
    address: str,
    healthy: bool,
    last_seen: str = "",
    detail_prefix: str = "",
) -> ServiceStatus:
    normalized_address = _normalize_address(address)
    gui_connectable, detail = _probe_gui_service(normalized_address)
    if detail_prefix:
        detail = f"{detail}; {detail_prefix}" if detail else detail_prefix
    return ServiceStatus(
        name=name,
        address=normalized_address,
        healthy=healthy,
        gui_connectable=gui_connectable,
        last_seen=last_seen,
        detail=detail,
    )

# Helper function to parse the raw response from the current List API of the manager and convert it to a list of ServiceStatus objects
# If the response cannot be parsed as the current ListResponse, 
# it falls back to parsing it as a legacy FetchServers response, 
# allowing compatibility with older manager versions while still checking GUI connectivity.   
# @param raw_response: The raw bytes response from the manager's List API to parse into service statuses 
def _current_services_from_raw(raw_response: bytes) -> list[ServiceStatus]:
    try:
        response = manager_pb2.ListResponse.FromString(raw_response)
    except DecodeError:
        return _flat_current_services(raw_response)

    services: list[ServiceStatus] = []
    for service in response.services:
        definition = service.definition
        services.append(
            _service_status(
                name=definition.name,
                address=definition.address,
                healthy=bool(service.healthy),
                last_seen=_format_timestamp(service.last_seen),
            )
        )
    return sorted(services, key=lambda service: service.name)

# Helper function to parse the raw response from the legacy FetchServers API
def _flat_current_services(raw_response: bytes) -> list[ServiceStatus]:
    services: list[ServiceStatus] = []
    for name, description, address in _decode_legacy_fetch_servers(raw_response):
        services.append(
            _service_status(
                name=name,
                address=address,
                healthy=True,
                detail_prefix=description,
            )
        )
    return sorted(services, key=lambda service: service.name)

# Helper function to format a gRPC RpcError into a user-friendly error message
def _format_rpc_error(label: str, address: str, exc: grpc.RpcError) -> str:
    code = exc.code().name if exc.code() is not None else "RPC_ERROR"
    details = exc.details() or str(exc)
    if "Connection refused" in details:
        return f"{label} unavailable at {address}: connection refused"
    if "Operation not permitted" in details:
        return f"{label} unavailable at {address}: local TCP was blocked"
    if "Exception deserializing response" in details:
        return (
            f"{label} protocol mismatch at {address}: the server answered with an "
            "incompatible protobuf response. Stop the process on that port or use "
            "the current h2pcontrol manager; direct targets still work."
        )
    return f"{label} unavailable at {address}: {code}: {details}"

# Main function to get the list of services from the manager, with error handling and legacy fallback
# @param manager_addr: The address of the manager to connect to (e.g. "localhost:50051")
# @param timeout: The timeout in seconds for the gRPC calls to the manager and services
def get_services(manager_addr: str, timeout: float = 5.0) -> list[ServiceStatus]:
    channel = grpc.insecure_channel(manager_addr)
    try:
        try:
            grpc.channel_ready_future(channel).result(timeout=timeout)
        except grpc.FutureTimeoutError as exc:
            raise RuntimeError(
                f"Manager unavailable at {manager_addr}. "
                "Start the h2pcontrol manager, pass --manager-addr, or connect with --target."
            ) from exc

        list_services = channel.unary_unary(
            "/h2pcontrol.manager.v1.ManagerService/List",
            request_serializer=manager_pb2.ListRequest.SerializeToString,
            response_deserializer=lambda data: data,
        )
        try:
            raw_response = list_services(manager_pb2.ListRequest(), timeout=timeout)
        except grpc.RpcError as exc:
            if exc.code() in (grpc.StatusCode.INTERNAL, grpc.StatusCode.UNIMPLEMENTED):
                try:
                    return _legacy_services(channel, timeout)
                except Exception:
                    pass
            raise
        return _current_services_from_raw(raw_response)
    except grpc.RpcError as exc:
        raise RuntimeError(_format_rpc_error("Manager", manager_addr, exc)) from exc
    finally:
        channel.close()
    
