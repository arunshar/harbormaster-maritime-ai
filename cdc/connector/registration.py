"""Kafka Connect registration transport and readiness checks.

Connector configs contain ConfigProvider placeholders such as
``${dir:/dev/shm/secrets:password}``. Those bytes must reach Kafka Connect
unchanged. In particular, an unquoted shell heredoc expands the placeholder
before curl sends the request.
"""

from __future__ import annotations

import base64
import json
import math
import shlex
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from cdc.connector.config import (
    CONNECTOR_NAME,
    DATABASE_PASSWORD_REFERENCE,
    validate_connector_config,
)

UrlOpen = Callable[..., Any]


@dataclass(frozen=True)
class RegistrationResult:
    """Observed result after both the connector and its tasks are running."""

    http_status: int
    connector_state: str
    task_states: tuple[str, ...]


def encode_connector_config(body: dict[str, Any]) -> str:
    """Return the flat connector config as shell-safe base64 text."""
    validate_connector_config(body)
    payload = json.dumps(body["config"], separators=(",", ":"), sort_keys=True).encode()
    return base64.b64encode(payload).decode("ascii")


def build_ecs_exec_registration_command(
    body: dict[str, Any],
    *,
    connect_url: str = "http://localhost:8083",
) -> str:
    """Build a remote command whose payload cannot undergo shell expansion."""
    password = str(body.get("config", {}).get("database.password", ""))
    if password != DATABASE_PASSWORD_REFERENCE:
        raise ValueError(
            "ECS Exec registration requires the DirectoryConfigProvider password reference"
        )
    encoded = encode_connector_config(body)
    connector_name = str(body["name"])
    url = f"{connect_url.rstrip('/')}/connectors/{connector_name}/config"
    script = (
        f"printf '%s' {shlex.quote(encoded)} | base64 -d | "
        "curl --fail-with-body --silent --show-error --request PUT "
        "--header 'Content-Type: application/json' --data-binary @- "
        f"{shlex.quote(url)}"
    )
    return f"/bin/bash -c {shlex.quote(script)}"


def build_ecs_exec_readiness_command(
    *,
    connector_name: str = CONNECTOR_NAME,
    connect_url: str = "http://localhost:8083",
    timeout_s: float = 60.0,
    poll_interval_s: float = 1.0,
) -> str:
    """Build a bounded remote readiness check using the image's Python 3."""
    if timeout_s <= 0 or poll_interval_s < 0:
        raise ValueError(
            "readiness timing values must be non-negative and timeout must be positive"
        )
    status_url = f"{connect_url.rstrip('/')}/connectors/{connector_name}/status"
    python = f"""import json
import sys
import time
import urllib.error
import urllib.request

deadline = time.monotonic() + {timeout_s!r}
latest_connector = "UNKNOWN"
latest_tasks = ()
while time.monotonic() < deadline:
    try:
        with urllib.request.urlopen({status_url!r}, timeout=10) as response:
            status = json.load(response)
    except urllib.error.URLError:
        time.sleep({poll_interval_s!r})
        continue
    latest_connector = str(status["connector"]["state"])
    latest_tasks = tuple(str(task["state"]) for task in status.get("tasks", []))
    if latest_connector == "FAILED" or "FAILED" in latest_tasks:
        sys.exit(
            f"connector {connector_name} failed: "
            f"connector={{latest_connector}}, tasks={{latest_tasks}}"
        )
    if latest_connector == "RUNNING" and latest_tasks and all(
        state == "RUNNING" for state in latest_tasks
    ):
        print(f"connector={{latest_connector}}; tasks={{','.join(latest_tasks)}}")
        sys.exit(0)
    time.sleep({poll_interval_s!r})
sys.exit(
    f"connector {connector_name} did not reach RUNNING: "
    f"connector={{latest_connector}}, tasks={{latest_tasks}}"
)
"""
    encoded = base64.b64encode(python.encode()).decode("ascii")
    script = f"printf '%s' {shlex.quote(encoded)} | base64 -d | python3 -"
    return f"/bin/bash -c {shlex.quote(script)}"


def register_and_wait(
    connect_url: str,
    body: dict[str, Any],
    *,
    timeout_s: float = 30.0,
    poll_interval_s: float = 0.5,
    urlopen: UrlOpen = urllib.request.urlopen,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> RegistrationResult:
    """PUT a connector config and wait for connector and task readiness."""
    if not math.isfinite(timeout_s) or timeout_s <= 0:
        raise ValueError("registration timeout must be finite and positive")
    if not math.isfinite(poll_interval_s) or poll_interval_s <= 0:
        raise ValueError("registration poll interval must be finite and positive")
    validate_connector_config(body)
    connector_name = str(body["name"])
    connector_url = f"{connect_url.rstrip('/')}/connectors/{connector_name}"
    request = urllib.request.Request(
        f"{connector_url}/config",
        data=json.dumps(body["config"]).encode(),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    with urlopen(request, timeout=10) as response:
        http_status = int(response.status)

    deadline = monotonic() + timeout_s
    latest_connector_state = "UNKNOWN"
    latest_task_states: tuple[str, ...] = ()
    while True:
        remaining_s = deadline - monotonic()
        if remaining_s <= 0:
            break
        try:
            with urlopen(
                f"{connector_url}/status",
                timeout=max(min(10.0, remaining_s), 1e-6),
            ) as response:
                status = json.loads(response.read())
        except urllib.error.HTTPError as exc:
            # Distributed Connect can acknowledge the config PUT before the
            # connector is visible through the status endpoint. Retry only
            # that bounded 404 window; other HTTP failures remain actionable.
            if exc.code != 404:
                raise
        except urllib.error.URLError:
            # A worker rebalance can briefly reset the status connection after
            # the accepted PUT. The outer deadline keeps this retry bounded.
            pass
        else:
            latest_connector_state = str(status["connector"]["state"])
            latest_task_states = tuple(str(task["state"]) for task in status.get("tasks", []))
            if latest_connector_state == "FAILED" or "FAILED" in latest_task_states:
                raise RuntimeError(
                    f"connector {connector_name} failed: "
                    f"connector={latest_connector_state}, tasks={latest_task_states}"
                )
            if (
                latest_connector_state == "RUNNING"
                and latest_task_states
                and all(state == "RUNNING" for state in latest_task_states)
            ):
                return RegistrationResult(
                    http_status=http_status,
                    connector_state=latest_connector_state,
                    task_states=latest_task_states,
                )

        remaining_s = deadline - monotonic()
        if remaining_s <= 0:
            break
        sleep(min(poll_interval_s, remaining_s))

    raise TimeoutError(
        f"connector {connector_name} did not reach RUNNING within {timeout_s:g}s: "
        f"connector={latest_connector_state}, tasks={latest_task_states}"
    )
