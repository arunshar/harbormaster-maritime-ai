"""Regression coverage for provider-backed connector registration."""

from __future__ import annotations

import base64
import json
import math
import subprocess
import threading
import urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest
import yaml

from cdc.connector.config import DATABASE_PASSWORD_REFERENCE, build_connector_config
from cdc.connector.registration import (
    RegistrationResult,
    build_ecs_exec_readiness_command,
    build_ecs_exec_registration_command,
    encode_connector_config,
    register_and_wait,
)

ROOT = Path(__file__).parents[2]


class FakeResponse:
    def __init__(self, payload=None, *, status: int = 200):
        self.payload = payload
        self.status = status

    def read(self) -> bytes:
        return json.dumps(self.payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None


class FakeUrlOpen:
    def __init__(self, responses: list[FakeResponse | Exception]):
        self.responses = iter(responses)
        self.calls = []

    def __call__(self, target, *, timeout: float):
        self.calls.append((target, timeout))
        response = next(self.responses)
        if isinstance(response, Exception):
            raise response
        return response


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def _build() -> dict:
    return build_connector_config(db_host="postgres.hm-cdc.svc")


def test_registration_preserves_placeholder_and_waits_for_tasks():
    urlopen = FakeUrlOpen(
        [
            FakeResponse(status=201),
            FakeResponse({"connector": {"state": "RUNNING"}, "tasks": []}),
            FakeResponse({"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]}),
        ]
    )
    clock = FakeClock()

    result = register_and_wait(
        "http://connect:8083",
        _build(),
        poll_interval_s=0.25,
        urlopen=urlopen,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert result == RegistrationResult(201, "RUNNING", ("RUNNING",))
    request = urlopen.calls[0][0]
    sent = json.loads(request.data)
    assert sent["database.password"] == DATABASE_PASSWORD_REFERENCE


def test_registration_retries_transient_status_404_after_put():
    status_url = "http://connect:8083/connectors/harbormaster-postgres/status"
    urlopen = FakeUrlOpen(
        [
            FakeResponse(status=201),
            urllib.error.HTTPError(status_url, 404, "Not Found", hdrs=None, fp=None),
            FakeResponse({"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]}),
        ]
    )
    clock = FakeClock()

    result = register_and_wait(
        "http://connect:8083",
        _build(),
        poll_interval_s=0.25,
        urlopen=urlopen,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert result == RegistrationResult(201, "RUNNING", ("RUNNING",))
    assert len(urlopen.calls) == 3


def test_registration_does_not_retry_non_404_status_error():
    status_url = "http://connect:8083/connectors/harbormaster-postgres/status"
    urlopen = FakeUrlOpen(
        [
            FakeResponse(status=200),
            urllib.error.HTTPError(status_url, 500, "Internal Server Error", hdrs=None, fp=None),
        ]
    )

    with pytest.raises(urllib.error.HTTPError) as exc_info:
        register_and_wait(
            "http://connect:8083",
            _build(),
            urlopen=urlopen,
        )

    assert exc_info.value.code == 500
    assert len(urlopen.calls) == 2


def test_registration_persistent_status_404_stops_at_deadline():
    status_url = "http://connect:8083/connectors/harbormaster-postgres/status"
    urlopen = FakeUrlOpen(
        [
            FakeResponse(status=200),
            urllib.error.HTTPError(status_url, 404, "Not Found", hdrs=None, fp=None),
            urllib.error.HTTPError(status_url, 404, "Not Found", hdrs=None, fp=None),
        ]
    )
    clock = FakeClock()

    with pytest.raises(TimeoutError, match=r"connector=UNKNOWN, tasks=\(\)"):
        register_and_wait(
            "http://connect:8083",
            _build(),
            timeout_s=1,
            poll_interval_s=0.5,
            urlopen=urlopen,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )

    assert clock.now == 1
    assert [timeout for _, timeout in urlopen.calls] == [10, 1, 0.5]


def test_registration_retries_transient_connection_error():
    urlopen = FakeUrlOpen(
        [
            FakeResponse(status=200),
            urllib.error.URLError("connection reset"),
            FakeResponse({"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]}),
        ]
    )
    clock = FakeClock()

    result = register_and_wait(
        "http://connect:8083",
        _build(),
        timeout_s=1,
        poll_interval_s=0.25,
        urlopen=urlopen,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert result == RegistrationResult(200, "RUNNING", ("RUNNING",))
    assert clock.now == 0.25
    assert [timeout for _, timeout in urlopen.calls] == [10, 1, 0.75]


@pytest.mark.parametrize(
    "status",
    [
        {"connector": {"state": "FAILED"}, "tasks": []},
        {"connector": {"state": "RUNNING"}, "tasks": [{"state": "FAILED"}]},
    ],
)
def test_registration_fails_fast_on_failed_state(status):
    urlopen = FakeUrlOpen([FakeResponse(status=200), FakeResponse(status)])

    with pytest.raises(RuntimeError, match="connector harbormaster-postgres failed"):
        register_and_wait("http://connect:8083", _build(), urlopen=urlopen)


def test_registration_timeout_reports_last_observed_state():
    urlopen = FakeUrlOpen(
        [
            FakeResponse(status=200),
            FakeResponse({"connector": {"state": "STARTING"}, "tasks": []}),
        ]
    )
    clock = FakeClock()

    with pytest.raises(TimeoutError, match=r"connector=STARTING, tasks=\(\)"):
        register_and_wait(
            "http://connect:8083",
            _build(),
            timeout_s=1,
            poll_interval_s=1,
            urlopen=urlopen,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )


@pytest.mark.parametrize(
    ("timeout_s", "poll_interval_s"),
    [
        (0, 0.5),
        (-1, 0.5),
        (math.inf, 0.5),
        (math.nan, 0.5),
        (1, 0),
        (1, -1),
        (1, math.inf),
        (1, math.nan),
    ],
)
def test_registration_rejects_unbounded_timing(timeout_s, poll_interval_s):
    with pytest.raises(ValueError, match="registration (timeout|poll interval)"):
        register_and_wait(
            "http://connect:8083",
            _build(),
            timeout_s=timeout_s,
            poll_interval_s=poll_interval_s,
        )


def test_ecs_exec_command_base64_transport_preserves_placeholder():
    body = _build()
    encoded = encode_connector_config(body)
    decoded = json.loads(base64.b64decode(encoded))
    command = build_ecs_exec_registration_command(body)

    assert decoded["database.password"] == DATABASE_PASSWORD_REFERENCE
    assert encoded in command
    assert "${dir:" not in command
    assert "--data-binary @-" in command
    assert command.startswith("/bin/bash -c ")


def test_ecs_exec_command_survives_real_shell_and_http_transport():
    class CaptureHandler(BaseHTTPRequestHandler):
        received: bytes | None = None

        def do_PUT(self) -> None:
            length = int(self.headers["Content-Length"])
            type(self).received = self.rfile.read(length)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"{}")

        def log_message(self, format, *args) -> None:
            return None

    server = HTTPServer(("127.0.0.1", 0), CaptureHandler)
    server.timeout = 5
    thread = threading.Thread(target=server.handle_request)
    thread.start()
    try:
        connect_url = f"http://127.0.0.1:{server.server_port}"
        command = build_ecs_exec_registration_command(_build(), connect_url=connect_url)
        subprocess.run(["/bin/bash", "-c", command], check=True, capture_output=True, text=True)
    finally:
        thread.join(timeout=6)
        server.server_close()

    assert not thread.is_alive()
    assert CaptureHandler.received is not None
    sent = json.loads(CaptureHandler.received)
    assert sent["database.password"] == DATABASE_PASSWORD_REFERENCE


@pytest.mark.parametrize("password", ["do-not-transport", "${env:HM_PG_PASSWORD}"])
def test_ecs_exec_command_rejects_unsupported_password_source(password):
    body = _build()
    body["config"]["database.password"] = password

    with pytest.raises(ValueError, match="DirectoryConfigProvider password reference"):
        build_ecs_exec_registration_command(body)


def test_ecs_exec_readiness_command_polls_connector_and_tasks():
    class StatusHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            payload = {
                "connector": {"state": "RUNNING"},
                "tasks": [{"state": "RUNNING"}],
            }
            encoded = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format, *args) -> None:
            return None

    server = HTTPServer(("127.0.0.1", 0), StatusHandler)
    server.timeout = 5
    thread = threading.Thread(target=server.handle_request)
    thread.start()
    try:
        connect_url = f"http://127.0.0.1:{server.server_port}"
        command = build_ecs_exec_readiness_command(connect_url=connect_url)
        result = subprocess.run(
            ["/bin/bash", "-c", command], check=True, capture_output=True, text=True
        )
    finally:
        thread.join(timeout=6)
        server.server_close()

    assert not thread.is_alive()
    assert result.stdout.strip() == "connector=RUNNING; tasks=RUNNING"


@pytest.mark.parametrize(
    ("timeout_s", "poll_interval_s"),
    [(0, 1), (-1, 1), (1, -1)],
)
def test_ecs_exec_readiness_command_rejects_invalid_timing(timeout_s, poll_interval_s):
    with pytest.raises(ValueError, match="readiness timing values"):
        build_ecs_exec_readiness_command(
            timeout_s=timeout_s,
            poll_interval_s=poll_interval_s,
        )


def test_kind_manifest_exercises_directory_provider_bridge():
    manifest = ROOT / "deploy/k8s/cdc/30-connect.yaml"
    documents = list(yaml.safe_load_all(manifest.read_text()))
    deployment = next(doc for doc in documents if doc["kind"] == "Deployment")
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    env = {item["name"]: item.get("value") for item in container["env"]}
    startup = container["args"][0]

    assert env["CONNECT_CONFIG_PROVIDERS"] == "dir"
    assert (
        env["CONNECT_CONFIG_PROVIDERS_DIR_CLASS"]
        == "org.apache.kafka.common.config.provider.DirectoryConfigProvider"
    )
    assert env["HM_PG_PASSWORD"] == "hm_local_pw"
    assert "/dev/shm/secrets/password" in startup
    assert "exec /docker-entrypoint.sh start" in startup


def test_smoke_and_fixture_scripts_do_not_bypass_provider():
    for relative_path in ("scripts/cdc_smoke.py", "scripts/cdc_record_fixture.py"):
        source = (ROOT / relative_path).read_text()
        assert 'db_password="hm_local_pw"' not in source
