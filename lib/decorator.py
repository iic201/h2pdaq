from __future__ import annotations
import asyncio
import itertools
import time
import functools
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any
from daq_pipeline import DAQPipeline, DAQEvent

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
