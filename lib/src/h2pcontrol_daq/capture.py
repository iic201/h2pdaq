from __future__ import annotations
import functools
import json
import csv
import threading
import uuid
from datetime import datetime, time
import os
import h5py
import socket
import time
from typing import Any, Callable, Sequence

# TODO: The run id at the moment is not unique per client program, but per capture session. 
# We may want to make it unique per client program in the future.

_PROCESS_START_NS = time.time_ns()
_RAW_RUN_ID = f"{socket.gethostname()}_{os.getpid()}_{_PROCESS_START_NS}"
_RUN_ID = uuid.uuid5(uuid.NAMESPACE_DNS, _RAW_RUN_ID).hex

_DEFAULT_CSV_FIELDS = (
    "run_id",
    "timestamp",
    "source",
    "direction",
    "args",
    "kwargs",
    "result",
)
_CSV_LOCK = threading.Lock()


def _default_csv_path() -> str:
    os.makedirs("data", exist_ok=True)
    os.makedirs("data/csv", exist_ok=True)
    return f"data/csv/daq_capture_{_RUN_ID}.csv"

def _default_hdf5_path() -> str:
    os.makedirs("data", exist_ok=True)
    os.makedirs("data/hdf5", exist_ok=True)
    return f"data/hdf5/daq_capture_{_RUN_ID}.h5"


def get_run_id() -> str:
    return str(_RUN_ID)

def save_to_hdf5(filename: str, data: dict) -> None:
    with h5py.File(filename, "a") as hdf5_file:
        run_id = data.get("run_id", "unknown")
        timestamp = data.get("timestamp", datetime.now().isoformat())
        source = data.get("source", "unknown")
        direction = data.get("direction", "unknown")

        group_name = f"{run_id}_{timestamp}_{source}_{direction}"
        group = hdf5_file.require_group(group_name)

        for key, value in data.items():
            if key not in ("run_id", "timestamp", "source", "direction"):
                group.attrs[key] = json.dumps(value, default=str)


def save_to_csv(filename: str, data: dict, fieldnames: tuple[str, ...] | None = None) -> None:
    fields = fieldnames or tuple(data.keys())
    row = {name: data.get(name, "") for name in fields}

    with _CSV_LOCK:
        with open(filename, mode="a", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerow(row)


def _pick_args(args: Sequence[Any], indices: Sequence[int] | None):
    if indices is None:
        return args
    return [args[i] for i in indices if i < len(args)]

def _pick_kwargs(kwargs: dict, keys: Sequence[str] | None):
    if keys is None:
        return kwargs
    return {k: v for k, v in kwargs.items() if k in keys}

def capture(
        source: str, 
        direction: str = "both",
        in_args: Sequence[int] | None = None,
        in_kwargs: Sequence[str] | None = None,
    ):

    log_in = direction in ("in", "both")
    log_out = direction in ("out", "both")
    csv_target = _default_csv_path()
    hdf5_target = _default_hdf5_path()

    def decorator(func):

        @functools.wraps(func)
        async def wrapper(self_svc, *args, **kwargs):
            run_id = get_run_id()

            if log_in:
                picked_args = _pick_args(args, in_args)
                picked_kwargs = _pick_kwargs(kwargs, in_kwargs)
                json_args = json.dumps(picked_args, default=str)
                json_kwargs = json.dumps(picked_kwargs, default=str)
                timestamp = datetime.now().isoformat()
                print(
                    f"[DAQ][{run_id}] Captured IN event from {source}: "
                    f"args={json_args}, kwargs={json_kwargs} at {timestamp}"
                )
                save_to_csv(
                    csv_target,
                    {
                        "run_id": run_id,
                        "timestamp": timestamp,
                        "source": source,
                        "direction": "in",
                        "args": picked_args,
                        "kwargs": picked_kwargs,
                        "result": "",
                    },
                    _DEFAULT_CSV_FIELDS,
                )
                save_to_hdf5(hdf5_target, {
                    "run_id": run_id,
                    "timestamp": timestamp,
                    "source": source,
                    "direction": "in",
                    "args": picked_args,
                    "kwargs": picked_kwargs,
                })

            result = await func(self_svc, *args, **kwargs)

            if log_out:
                json_result = json.dumps(result, default=str)
                timestamp = datetime.now().isoformat()
                print(
                    f"[DAQ][{run_id}] Captured OUT event from {source}: "
                    f"result={json_result} at {timestamp}"
                )
                save_to_csv(
                    csv_target,
                    {
                        "run_id": run_id,
                        "timestamp": timestamp,
                        "source": source,
                        "direction": "out",
                        "args": "",
                        "kwargs": "",
                        "result": json_result,
                    },
                    _DEFAULT_CSV_FIELDS,
                )
                save_to_hdf5(hdf5_target, {
                    "run_id": run_id,
                    "timestamp": timestamp,
                    "source": source,
                    "direction": "out",
                    "args": "",
                    "kwargs": "",
                    "result": json_result,
                })
            return result

        return wrapper

    return decorator
