from __future__ import annotations
import functools
import json
import uuid
from datetime import datetime, time
import os
import socket
import time
from typing import Any, Sequence
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message
from .pipeline import LocalDAQ
from .models import PendingEvent

# TODO: The run id at the moment is not unique per client program, but per capture session. 
# We may want to make it unique per client program in the future.

_PROCESS_START_NS = time.time_ns()
_RAW_RUN_ID = f"{socket.gethostname()}_{os.getpid()}_{_PROCESS_START_NS}"
_RUN_ID = uuid.uuid5(uuid.NAMESPACE_DNS, _RAW_RUN_ID).hex

def get_run_id() -> str:
    return str(_RUN_ID)

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
    ):

    log_in = direction in ("in", "both")
    log_out = direction in ("out", "both")
    producer_id = f"{socket.gethostname()}_{os.getpid()}"

    def decorator(func):

        @functools.wraps(func)
        async def wrapper(self_svc, *args, **kwargs):
            run_id = get_run_id()

            if log_in:
                picked_args = _pick_args(args, in_args)
                picked_kwargs = _pick_kwargs(kwargs, in_kwargs)
                timestamp = datetime.now().isoformat()
                data = {
                    "args": _normalize_for_json(picked_args),
                    "kwargs": _normalize_for_json(picked_kwargs),
                }
                pending_event = PendingEvent(
                    event_id=0,
                    timestamp=timestamp,
                    run_id=run_id,
                    producer_id=producer_id,
                    source=source,
                    method=func.__name__,
                    direction="in",
                    data=data,
                )
                daq.publish_pending_event(pending_event)

            result = await func(self_svc, *args, **kwargs)

            if log_out:
                timestamp = datetime.now().isoformat()
                data = {"result": _normalize_for_json(result)}
                pending_event = PendingEvent(
                    event_id=0,
                    timestamp=timestamp,
                    run_id=run_id,
                    producer_id=producer_id,
                    source=source,
                    method=func.__name__,
                    direction="out",
                    data=data,
                )
                daq.publish_pending_event(pending_event)
            
            print(f"[DAQ]Captured event for method {func.__name__}")

            return result

        return wrapper

    return decorator
