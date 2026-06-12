#!/usr/bin/env python3
from __future__ import annotations

import json
import secrets
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

CONTAINER_NAME = "h2pcontrol-influxdb"
IMAGE = "influxdb:2.7"
HOST_PORT = 8086
ORG = "beyer-labs"
BUCKET = "test"
ADMIN_USER = "admin"

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"

### 
# Runs a subprocess command and returns the CompletedProcess result.
# By default, it captures the output and checks for errors, but these can be disabled.
###
def run(cmd: list[str], check: bool = True, capture_output: bool = True):
    return subprocess.run(
        cmd,
        check=check,
        text=True,
        capture_output=capture_output,
    )


###
# Tests that docker is available,
# if no version is found, exits with an error.
# if docker command is not even available it also exits with an error.
### 
def require_docker() -> None:
    try:
        run(["docker", "--version"])
    except FileNotFoundError:
        print("[InfluxDB] Docker is required but was not found in PATH.")
        sys.exit(1)
    except subprocess.CalledProcessError:
        print("[InfluxDB] Docker is required but is not available.")
        sys.exit(1)

    info = run(["docker", "info"], check=False)
    if info.returncode != 0:
        message = info.stderr.strip() or "Docker daemon is not running."
        print(f"[InfluxDB] {message}")
        sys.exit(1)


def container_names(all_containers: bool) -> set[str]:
    cmd = ["docker", "ps"]
    if all_containers:
        cmd.append("-a")
    cmd.extend(["--format", "{{.Names}}"])
    result = run(cmd, check=False)
    if result.returncode == 0:
        return {line.strip() for line in result.stdout.splitlines() if line.strip()}

    id_cmd = ["docker", "ps"]
    if all_containers:
        id_cmd.append("-a")
    id_cmd.append("--quiet")
    id_result = run(id_cmd, check=False)
    if id_result.returncode != 0:
        raise RuntimeError(id_result.stderr.strip() or "Failed to list docker containers")

    ids = [line.strip() for line in id_result.stdout.splitlines() if line.strip()]
    if not ids:
        return set()

    inspect = run(["docker", "inspect"] + ids, check=False)
    if inspect.returncode != 0:
        raise RuntimeError(inspect.stderr.strip() or "Failed to inspect docker containers")

    try:
        payload = json.loads(inspect.stdout)
    except json.JSONDecodeError:
        raise RuntimeError("Failed to parse docker inspect output")

    names: set[str] = set()
    for item in payload:
        name = item.get("Name")
        if isinstance(name, str) and name:
            names.add(name.lstrip("/"))
    return names


def wait_for_influx(timeout_s: int = 60) -> None:
    url = f"http://localhost:{HOST_PORT}/health"
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return
        except Exception:
            time.sleep(1)
    print("Timed out waiting for InfluxDB to become healthy.")
    sys.exit(1)

### 
# Reads environment variables from a file.
###
def read_env(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    env: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip()
    return env


def update_env(path: Path, updates: dict[str, str]) -> None:
    lines: list[str] = []
    if path.exists():
        lines = path.read_text(encoding="utf-8").splitlines()

    seen: set[str] = set()
    new_lines: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            new_lines.append(line)
            continue
        key = line.split("=", 1)[0].strip()
        if key in updates:
            new_lines.append(f"{key}={updates[key]}")
            seen.add(key)
        else:
            new_lines.append(line)

    for key, value in updates.items():
        if key not in seen:
            new_lines.append(f"{key}={value}")

    path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


def run_influx(args: list[str], token: str, input_text: str | None = None, check: bool = True):
    cmd = ["docker", "exec", "-i", CONTAINER_NAME, "influx"] + args + ["-t", token]
    return subprocess.run(
        cmd,
        input=input_text,
        text=True,
        capture_output=True,
        check=check,
    )


def create_org_and_bucket(token: str) -> None:
    result = run_influx(["org", "create", "--name", ORG], token, check=False)
    if result.returncode != 0 and "already exists" not in result.stderr.lower():
        raise RuntimeError(result.stderr.strip() or "Failed to create org")

    result = run_influx(
        ["bucket", "create", "--name", BUCKET, "--org", ORG],
        token,
        check=False,
    )
    if result.returncode != 0 and "already exists" not in result.stderr.lower():
        raise RuntimeError(result.stderr.strip() or "Failed to create bucket")


def write_mock_data(token: str) -> None:
    now = time.time_ns()
    lines = "\n".join(
        [
            f"test,source=mock value=1i {now}",
            f"test,source=mock value=2i {now + 1}",
            f"test,source=mock value=3i {now + 2}",
        ]
    )
    run_influx(
        ["write", "--org", ORG, "--bucket", BUCKET, "--precision", "ns"],
        token,
        input_text=lines,
    )


def main() -> None:
    require_docker()

    env = read_env(ENV_PATH)
    token = env.get("INFLUXDB_ADMIN_TOKEN")
    admin_user = env.get("INFLUXDB_ADMIN_USER", ADMIN_USER)
    admin_password = env.get("INFLUXDB_ADMIN_PASSWORD")

    existing = container_names(all_containers=True)
    running = container_names(all_containers=False)

    if CONTAINER_NAME in existing:
        if not token:
            print(
                "Existing InfluxDB container found but INFLUXDB_ADMIN_TOKEN is missing in .env."
            )
            print("Remove the container or set the token in .env and retry.")
            sys.exit(1)
        if CONTAINER_NAME not in running:
            run(["docker", "start", CONTAINER_NAME], capture_output=False)
    else:
        if not token:
            token = secrets.token_hex(32)
        if not admin_password:
            admin_password = secrets.token_urlsafe(16)

        run(["docker", "pull", IMAGE], capture_output=False)
        run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                CONTAINER_NAME,
                "-p",
                f"{HOST_PORT}:8086",
                "-v",
                "h2pcontrol-influxdb-data:/var/lib/influxdb2",
                "-v",
                "h2pcontrol-influxdb-config:/etc/influxdb2",
                "-e",
                "DOCKER_INFLUXDB_INIT_MODE=setup",
                "-e",
                f"DOCKER_INFLUXDB_INIT_USERNAME={admin_user}",
                "-e",
                f"DOCKER_INFLUXDB_INIT_PASSWORD={admin_password}",
                "-e",
                f"DOCKER_INFLUXDB_INIT_ORG={ORG}",
                "-e",
                f"DOCKER_INFLUXDB_INIT_BUCKET={BUCKET}",
                "-e",
                f"DOCKER_INFLUXDB_INIT_ADMIN_TOKEN={token}",
                IMAGE,
            ],
            capture_output=False,
        )

    wait_for_influx()
    create_org_and_bucket(token)
    write_mock_data(token)

    update_env(
        ENV_PATH,
        {
            "INFLUXDB_URL": f"http://localhost:{HOST_PORT}",
            "INFLUXDB_ORG": ORG,
            "INFLUXDB_BUCKET": BUCKET,
            "INFLUXDB_ADMIN_TOKEN": token,
            "INFLUXDB_ADMIN_USER": admin_user,
            "INFLUXDB_ADMIN_PASSWORD": admin_password or "",
        },
    )

    print("InfluxDB is ready.")
    print(f"Admin token: {token}")


if __name__ == "__main__":
    main()
