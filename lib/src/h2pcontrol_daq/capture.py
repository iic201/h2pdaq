from __future__ import annotations
import functools
import inspect
import json
from datetime import datetime
import os
import socket
from typing import Any, Mapping, Sequence
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message
from .pipeline import (
    LocalDAQ,
    get_run_id,
    _normalize_event_data,
    _normalize_save_formats,
    _validate_enabled_save_formats,
)
from .models import DAQSaveFormat, PendingEvent

def _pick_args(args: Sequence[Any], indices: Sequence[int] | None):
    if indices is None:
        return args
    return [args[i] for i in indices if i < len(args)]

def _pick_kwargs(kwargs: dict, keys: Sequence[str] | None):
    if keys is None:
        return kwargs
    return {k: v for k, v in kwargs.items() if k in keys}

def _normalize_for_json(value: Any) -> Any:
    if isinstance(value, Message):
        return MessageToDict(value, preserving_proto_field_name=True)
    if isinstance(value, dict):
        return {k: _normalize_for_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_for_json(v) for v in value]
    try:
        json.dumps(value)
        return value
    except TypeError:
        return str(value)

def capture(
        daq: LocalDAQ,
        source: str, 
        direction: str = "both",
        in_args: Sequence[int] | None = None,
        in_kwargs: Sequence[str] | None = None,
        tags: Mapping[str, Any] | None = None,
        unit: str = "",
        metadata: Mapping[str, Any] | None = None,
        save_formats: Sequence[DAQSaveFormat | str] | None = None,
    ):

    log_in = direction in ("in", "both")
    log_out = direction in ("out", "both")
    producer_id = f"{socket.gethostname()}_{os.getpid()}"
    capture_tags = dict(tags or {})
    capture_save_formats = _normalize_save_formats(save_formats) if save_formats is not None else None
    if capture_save_formats is not None:
        _validate_enabled_save_formats(capture_save_formats, daq._enabled_save_formats)

    def decorator(func):

        def publish_input(args, kwargs, run_id: str) -> None:
            if log_in:
                picked_args = _pick_args(args, in_args)
                picked_kwargs = _pick_kwargs(kwargs, in_kwargs)
                timestamp = datetime.now().isoformat()
                data = {
                    "args": _normalize_for_json(picked_args),
                    "kwargs": _normalize_for_json(picked_kwargs),
                }
                data = _normalize_event_data(data, metadata=metadata, tags=capture_tags)
                pending_event = PendingEvent(
                    event_id=0,
                    timestamp=timestamp,
                    run_id=run_id,
                    producer_id=producer_id,
                    source=source,
                    method=func.__name__,
                    direction="in",
                    data=data,
                    tags=capture_tags,
                    save_formats=capture_save_formats,
                    normalized=True,
                )
                daq.publish_pending_event(pending_event)

        def publish_output(result, run_id: str) -> None:
            if log_out:
                timestamp = datetime.now().isoformat()
                data = _normalize_event_data(
                    {"result": _normalize_for_json(result)},
                    unit=unit,
                    metadata=metadata,
                    tags=capture_tags,
                )
                pending_event = PendingEvent(
                    event_id=0,
                    timestamp=timestamp,
                    run_id=run_id,
                    producer_id=producer_id,
                    source=source,
                    method=func.__name__,
                    direction="out",
                    data=data,
                    tags=capture_tags,
                    save_formats=capture_save_formats,
                    normalized=True,
                )
                daq.publish_pending_event(pending_event)

        if inspect.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(self_svc, *args, **kwargs):
                run_id = get_run_id()
                publish_input(args, kwargs, run_id)
                result = await func(self_svc, *args, **kwargs)
                publish_output(result, run_id)
                return result

            return async_wrapper

        @functools.wraps(func)
        def sync_wrapper(self_svc, *args, **kwargs):
            run_id = get_run_id()
            publish_input(args, kwargs, run_id)
            result = func(self_svc, *args, **kwargs)
            publish_output(result, run_id)
            return result

        return sync_wrapper

    return decorator
