"""Capture Flink 1.20 backpressure evidence during one bound W4 load run.

This helper uses the read-only REST API behind the Apache Flink dashboard. It
does not call an AWS API. A human must create ``FLINK_DASHBOARD_URL`` with the
Managed Service for Apache Flink ``CreateApplicationPresignedUrl`` operation.

The presigned authorization URL is opened once with an HTTPS-only redirect
handler and a cookie jar. After authorization, every REST request is a GET to
the exact final HTTPS origin and dashboard base path, and redirects are
rejected. The supported calls are deliberately narrow:

* ``/jobs/overview``
* ``/jobs/{job_id}``
* ``/jobs/{job_id}/vertices/{vertex_id}/backpressure``

Example for the human-run W4 window:

    FLINK_DASHBOARD_URL="$FLINK_DASHBOARD_URL" \
    W4_ARTIFACT_PATH=artifacts/w4/<stamp>/kinesis-load.json \
    W4_RUN_ID=<fresh-uuid4> \
    W4_FLINK_BACKPRESSURE_ARTIFACT_PATH=artifacts/w4/<stamp>/flink-backpressure.json \
    python scripts/capture_flink_backpressure.py --poll-interval-seconds 2

The output reports observations and three evaluated conditions. It never
labels the run as a pass: the final three pre-burst sample maxima <= 0.1,
burst max ratio > 0.1, and the final three post-burst sample maxima <= 0.1.
"""

from __future__ import annotations

import argparse
import http.cookiejar
import json
import math
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
MAX_JSON_BYTES = 2 * 1024 * 1024
MIN_POLL_INTERVAL_SECONDS = 0.5
MAX_POLL_INTERVAL_SECONDS = 30.0
MAX_CAPTURE_DURATION_SECONDS = 900.0
MAX_WAIT_TIMEOUT_SECONDS = 300.0
MAX_REQUEST_TIMEOUT_SECONDS = 30.0
MAX_BACKPRESSURE_READY_TIMEOUT_SECONDS = 30.0
STABLE_TAIL_SAMPLE_COUNT = 3
HEX_ID = re.compile(r"^[0-9a-fA-F]{32}$")
API_PATH = re.compile(
    r"^/jobs/(?:overview|[0-9a-fA-F]{32}(?:/vertices/[0-9a-fA-F]{32}/backpressure)?)$"
)
LEVELS = {"ok", "low", "high"}


class EvidenceError(RuntimeError):
    """The live inputs or observations cannot support trustworthy evidence."""


@dataclass(frozen=True)
class LoadRun:
    """Validated identity and timing for one fresh load-generator run."""

    run_id: str
    artifact_path: Path
    started_at: datetime
    duration_seconds: float
    steady_rps: float
    burst_rps: float
    burst_start_seconds: float
    burst_end_seconds: float
    ramp_seconds: float

    @property
    def burst_window_end_seconds(self) -> float:
        return self.burst_end_seconds + self.ramp_seconds

    @property
    def profile(self) -> dict[str, float]:
        return {
            "steady_rps": self.steady_rps,
            "burst_rps": self.burst_rps,
            "burst_start_s": self.burst_start_seconds,
            "burst_end_s": self.burst_end_seconds,
            "ramp_s": self.ramp_seconds,
        }


@dataclass(frozen=True)
class CaptureSettings:
    """Finite, bounded runtime settings for one evidence capture."""

    poll_interval_seconds: float
    capture_until_seconds: float
    backpressure_ready_timeout_seconds: float = 10.0

    def __post_init__(self) -> None:
        _bounded_number(
            self.poll_interval_seconds,
            "poll_interval_seconds",
            MIN_POLL_INTERVAL_SECONDS,
            MAX_POLL_INTERVAL_SECONDS,
        )
        _bounded_number(
            self.capture_until_seconds,
            "capture_until_seconds",
            MIN_POLL_INTERVAL_SECONDS,
            MAX_CAPTURE_DURATION_SECONDS,
        )
        _bounded_number(
            self.backpressure_ready_timeout_seconds,
            "backpressure_ready_timeout_seconds",
            1.0,
            MAX_BACKPRESSURE_READY_TIMEOUT_SECONDS,
        )


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_utc(value: Any, name: str) -> datetime:
    if not isinstance(value, str):
        raise EvidenceError(f"{name} must be an ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise EvidenceError(f"{name} must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None:
        raise EvidenceError(f"{name} must include a timezone")
    return parsed.astimezone(UTC)


def _number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EvidenceError(f"{name} must be a number")
    converted = float(value)
    if not math.isfinite(converted):
        raise EvidenceError(f"{name} must be finite")
    return converted


def _bounded_number(value: Any, name: str, minimum: float, maximum: float) -> float:
    converted = _number(value, name)
    if not minimum <= converted <= maximum:
        raise EvidenceError(f"{name} must be in [{minimum}, {maximum}], got {converted}")
    return converted


def validate_run_id(value: str) -> str:
    """Require the canonical UUID4 shared by all W4 evidence helpers."""
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as error:
        raise EvidenceError("expected_run_id must be a canonical UUID4") from error
    if parsed.version != 4 or str(parsed) != value:
        raise EvidenceError("expected_run_id must be a canonical UUID4")
    return value


def _hostname_is_aws(hostname: str) -> bool:
    hostname = hostname.rstrip(".").lower()
    return hostname.endswith(".amazonaws.com") or hostname.endswith(".amazonaws.com.cn")


def validate_authorization_url(value: str) -> str:
    """Accept only a standard-port AWS HTTPS presigned dashboard URL."""
    try:
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
    except (TypeError, ValueError) as error:
        raise EvidenceError("FLINK_DASHBOARD_URL is malformed") from error
    if parsed.scheme != "https":
        raise EvidenceError("FLINK_DASHBOARD_URL must use HTTPS")
    if not parsed.hostname or not _hostname_is_aws(parsed.hostname):
        raise EvidenceError("FLINK_DASHBOARD_URL must use an amazonaws.com host")
    if parsed.username is not None or parsed.password is not None:
        raise EvidenceError("FLINK_DASHBOARD_URL must not contain URL credentials")
    if port not in (None, 443):
        raise EvidenceError("FLINK_DASHBOARD_URL must use the standard HTTPS port")
    if parsed.fragment:
        raise EvidenceError("FLINK_DASHBOARD_URL must not contain a fragment")
    return value


def _https_origin(value: str, name: str) -> str:
    try:
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
    except (TypeError, ValueError) as error:
        raise EvidenceError(f"{name} is malformed") from error
    if parsed.scheme != "https" or not parsed.hostname:
        raise EvidenceError(f"{name} must resolve to an HTTPS origin")
    if parsed.username is not None or parsed.password is not None:
        raise EvidenceError(f"{name} must not contain URL credentials")
    if port not in (None, 443):
        raise EvidenceError(f"{name} must use the standard HTTPS port")
    return f"https://{parsed.hostname.lower()}"


def _dashboard_api_base_path(value: str) -> str:
    """Derive the path prefix that AWS assigned to this dashboard session."""
    try:
        parsed = urllib.parse.urlsplit(value)
    except (TypeError, ValueError) as error:
        raise EvidenceError("authorized dashboard URL is malformed") from error
    path = parsed.path or "/"
    if "%" in path or "\\" in path:
        raise EvidenceError("authorized dashboard path contains ambiguous encoding")
    parts = path.split("/")
    if any(part in {".", ".."} for part in parts):
        raise EvidenceError("authorized dashboard path contains a dot segment")
    if not path.endswith("/"):
        path = path.rsplit("/", 1)[0] + "/"
    base_path = path.rstrip("/")
    return "" if base_path == "" else base_path


class HTTPSOnlyRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Allow redirects during the one-time authorization, but never to HTTP."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        target = urllib.parse.urljoin(req.full_url, newurl)
        try:
            parsed = urllib.parse.urlsplit(target)
            port = parsed.port
        except ValueError as error:
            raise EvidenceError("dashboard authorization redirect is malformed") from error
        if parsed.scheme != "https" or not parsed.hostname:
            raise EvidenceError("dashboard authorization refused a non-HTTPS redirect")
        if parsed.username is not None or parsed.password is not None:
            raise EvidenceError("dashboard authorization redirect contains URL credentials")
        if port not in (None, 443):
            raise EvidenceError("dashboard authorization redirect uses a nonstandard port")
        return super().redirect_request(req, fp, code, msg, headers, target)


class RejectRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Reject every redirect after the final dashboard origin is established."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        raise EvidenceError("dashboard REST API redirect refused")


class DashboardSession:
    """Read-only client pinned to one dashboard HTTPS origin and base path."""

    def __init__(
        self,
        origin: str,
        opener: Any,
        timeout_seconds: float,
        api_base_path: str = "",
    ) -> None:
        self.origin = _https_origin(origin, "dashboard origin")
        if api_base_path and (
            not api_base_path.startswith("/")
            or api_base_path.endswith("/")
            or "?" in api_base_path
            or "#" in api_base_path
        ):
            raise EvidenceError("dashboard API base path is malformed")
        self.api_base_path = api_base_path
        self._opener = opener
        self._timeout_seconds = _bounded_number(
            timeout_seconds,
            "request_timeout_seconds",
            1.0,
            MAX_REQUEST_TIMEOUT_SECONDS,
        )

    @classmethod
    def authorize(cls, authorization_url: str, timeout_seconds: float = 10.0) -> DashboardSession:
        """Redeem the human-generated URL once, then disable all redirects."""
        authorization_url = validate_authorization_url(authorization_url)
        timeout_seconds = _bounded_number(
            timeout_seconds,
            "request_timeout_seconds",
            1.0,
            MAX_REQUEST_TIMEOUT_SECONDS,
        )
        cookies = http.cookiejar.CookieJar()
        authorization_opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(cookies),
            HTTPSOnlyRedirectHandler(),
        )
        request = urllib.request.Request(
            authorization_url,
            headers={"User-Agent": "harbormaster-w4-evidence/1"},
            method="GET",
        )
        try:
            with authorization_opener.open(request, timeout=timeout_seconds) as response:
                final_url = response.geturl()
        except urllib.error.HTTPError as error:
            raise EvidenceError(f"dashboard authorization failed with HTTP {error.code}") from error
        except urllib.error.URLError as error:
            raise EvidenceError("dashboard authorization request failed") from error

        origin = _https_origin(final_url, "authorized dashboard URL")
        api_base_path = _dashboard_api_base_path(final_url)
        api_opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(cookies),
            RejectRedirectHandler(),
        )
        return cls(origin, api_opener, timeout_seconds, api_base_path)

    def get_json(self, path: str) -> dict[str, Any]:
        """GET one allowlisted Flink endpoint from the exact authorized origin."""
        if not API_PATH.fullmatch(path):
            raise EvidenceError(f"dashboard REST path is not allowlisted: {path}")
        url = f"{self.origin}{self.api_base_path}{path}"
        if _https_origin(url, "dashboard REST URL") != self.origin:
            raise EvidenceError("dashboard REST URL changed origin")
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "harbormaster-w4-evidence/1",
            },
            method="GET",
        )
        try:
            with self._opener.open(request, timeout=self._timeout_seconds) as response:
                status = response.getcode()
                response_origin = _https_origin(response.geturl(), "dashboard REST response URL")
                if status != 200:
                    raise EvidenceError(f"dashboard REST GET {path} returned HTTP {status}")
                if response_origin != self.origin:
                    raise EvidenceError("dashboard REST response changed origin")
                if urllib.parse.urlsplit(response.geturl()).path != urllib.parse.urlsplit(url).path:
                    raise EvidenceError("dashboard REST response changed path")
                body = response.read(MAX_JSON_BYTES + 1)
        except EvidenceError:
            raise
        except urllib.error.HTTPError as error:
            raise EvidenceError(f"dashboard REST GET {path} returned HTTP {error.code}") from error
        except urllib.error.URLError as error:
            raise EvidenceError(f"dashboard REST GET {path} failed") from error
        if len(body) > MAX_JSON_BYTES:
            raise EvidenceError(f"dashboard REST GET {path} exceeded the JSON size limit")
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise EvidenceError(f"dashboard REST GET {path} returned malformed JSON") from error
        if not isinstance(payload, dict):
            raise EvidenceError(f"dashboard REST GET {path} must return a JSON object")
        return payload


def _load_identity(payload: dict[str, Any], path: Path, expected_run_id: str) -> str:
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise EvidenceError("load artifact schema_version must equal 1")
    if payload.get("run_id") != expected_run_id:
        raise EvidenceError("load artifact run_id does not match expected_run_id")
    value = payload.get("load_artifact")
    if not isinstance(value, str):
        raise EvidenceError("load artifact is missing load_artifact")
    if Path(value).expanduser().resolve() != path:
        raise EvidenceError("load artifact is bound to a different path")
    status = payload.get("status")
    if not isinstance(status, str):
        raise EvidenceError("load artifact is missing status")
    return status


def validate_load_payload(
    payload: dict[str, Any],
    artifact_path: Path,
    expected_run_id: str,
    *,
    now: datetime,
    max_start_age_seconds: float,
) -> LoadRun:
    """Validate a fresh running load and derive its burst bounds."""
    expected_run_id = validate_run_id(expected_run_id)
    artifact_path = artifact_path.expanduser().resolve()
    if not isinstance(payload, dict):
        raise EvidenceError("load artifact must contain a JSON object")
    status = _load_identity(payload, artifact_path, expected_run_id)
    if status != "running":
        raise EvidenceError(f"load artifact must be running, got {status}")
    max_start_age_seconds = _bounded_number(
        max_start_age_seconds,
        "max_load_start_age_seconds",
        1.0,
        MAX_WAIT_TIMEOUT_SECONDS,
    )
    started_at = _parse_utc(payload.get("started_at"), "load artifact started_at")
    age_seconds = (now.astimezone(UTC) - started_at).total_seconds()
    if age_seconds < 0:
        raise EvidenceError("load artifact started_at is in the future")
    if age_seconds > max_start_age_seconds:
        raise EvidenceError(f"load artifact is stale: age_seconds={age_seconds:.3f}")

    duration = _bounded_number(
        payload.get("duration_seconds"),
        "load artifact duration_seconds",
        MIN_POLL_INTERVAL_SECONDS,
        MAX_CAPTURE_DURATION_SECONDS,
    )
    profile = payload.get("profile")
    if not isinstance(profile, dict):
        raise EvidenceError("load artifact is missing profile")
    steady = _number(profile.get("steady_rps"), "profile steady_rps")
    burst = _number(profile.get("burst_rps"), "profile burst_rps")
    burst_start = _number(profile.get("burst_start_s"), "profile burst_start_s")
    burst_end = _number(profile.get("burst_end_s"), "profile burst_end_s")
    ramp = _number(profile.get("ramp_s"), "profile ramp_s")
    if steady < 0 or burst <= steady:
        raise EvidenceError("load profile must have burst_rps > steady_rps >= 0")
    if burst_start <= 0:
        raise EvidenceError("load profile burst_start_s must be > 0 for a pre-burst sample")
    if ramp < 0 or burst_end < burst_start + ramp:
        raise EvidenceError("load profile has invalid burst or ramp bounds")
    if burst_end + ramp > duration:
        raise EvidenceError("load duration does not cover burst_end_s + ramp_s")
    if age_seconds >= burst_start:
        raise EvidenceError("load artifact is too old to capture the pre-burst interval")
    return LoadRun(
        run_id=expected_run_id,
        artifact_path=artifact_path,
        started_at=started_at,
        duration_seconds=duration,
        steady_rps=steady,
        burst_rps=burst,
        burst_start_seconds=burst_start,
        burst_end_seconds=burst_end,
        ramp_seconds=ramp,
    )


def _read_json_file(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as error:
        raise EvidenceError(f"load artifact contains malformed JSON: {path}") from error
    if not isinstance(payload, dict):
        raise EvidenceError("load artifact must contain a JSON object")
    return payload


def wait_for_fresh_load(
    artifact_path: Path,
    expected_run_id: str,
    *,
    timeout_seconds: float = 120.0,
    max_start_age_seconds: float = 120.0,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    utc_now: Callable[[], datetime] = lambda: datetime.now(UTC),
    reader: Callable[[Path], dict[str, Any]] = _read_json_file,
) -> LoadRun:
    """Wait for the exact load artifact to transition preparing -> running."""
    validate_run_id(expected_run_id)
    artifact_path = artifact_path.expanduser().resolve()
    timeout_seconds = _bounded_number(
        timeout_seconds,
        "load_wait_timeout_seconds",
        1.0,
        MAX_WAIT_TIMEOUT_SECONDS,
    )
    deadline = monotonic() + timeout_seconds
    while monotonic() < deadline:
        try:
            payload = reader(artifact_path)
        except FileNotFoundError:
            sleep(min(0.2, max(0.0, deadline - monotonic())))
            continue
        status = _load_identity(payload, artifact_path, expected_run_id)
        if status == "running":
            return validate_load_payload(
                payload,
                artifact_path,
                expected_run_id,
                now=utc_now(),
                max_start_age_seconds=max_start_age_seconds,
            )
        if status != "preparing":
            raise EvidenceError(f"load artifact entered terminal status {status}")
        sleep(min(0.2, max(0.0, deadline - monotonic())))
    raise EvidenceError(f"load artifact did not become running within {timeout_seconds}s")


def _hex_id(value: Any, name: str) -> str:
    if not isinstance(value, str) or not HEX_ID.fullmatch(value):
        raise EvidenceError(f"{name} must be a 32-character hexadecimal ID")
    return value.lower()


def discover_running_job(client: DashboardSession) -> str:
    """Select exactly one RUNNING job from the Flink job overview."""
    payload = client.get_json("/jobs/overview")
    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        raise EvidenceError("/jobs/overview is missing jobs")
    running: list[str] = []
    for index, job in enumerate(jobs):
        if not isinstance(job, dict):
            raise EvidenceError(f"jobs[{index}] must be an object")
        job_id = _hex_id(job.get("jid"), f"jobs[{index}].jid")
        state = job.get("state")
        if not isinstance(state, str):
            raise EvidenceError(f"jobs[{index}].state must be a string")
        if state == "RUNNING":
            running.append(job_id)
    if len(running) != 1:
        raise EvidenceError(f"expected exactly one RUNNING Flink job, found {len(running)}")
    return running[0]


def _alias(payload: dict[str, Any], left: str, right: str, name: str) -> Any:
    left_value = payload.get(left)
    right_value = payload.get(right)
    if left_value is not None and right_value is not None and left_value != right_value:
        raise EvidenceError(f"{name} aliases disagree")
    value = left_value if left_value is not None else right_value
    if value is None:
        raise EvidenceError(f"{name} is missing")
    return value


def _level(payload: dict[str, Any], name: str) -> str:
    value = _alias(payload, "backpressure-level", "backpressureLevel", name)
    if not isinstance(value, str) or value.lower() not in LEVELS:
        raise EvidenceError(f"{name} must be one of ok, low, high")
    return value.lower()


def _diagnostic_ratio(value: Any, name: str) -> tuple[float | None, str | None]:
    """Keep valid busy/idle ratios without rejecting Flink's documented NaN."""
    if value is None:
        return None, "null"
    if isinstance(value, str) and value.lower() in {
        "nan",
        "infinity",
        "+infinity",
        "-infinity",
    }:
        return None, "non_finite"
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EvidenceError(f"{name} must be a number")
    converted = float(value)
    if not math.isfinite(converted):
        return None, "non_finite"
    if not 0.0 <= converted <= 1.0:
        return None, "out_of_range"
    return converted, None


def parse_vertex_backpressure(
    payload: dict[str, Any], vertex_id: str, vertex_name: str
) -> dict[str, Any]:
    """Validate and retain the Flink 1.20 per-vertex levels and ratios."""
    if payload.get("status") != "ok":
        raise EvidenceError(f"backpressure status for vertex {vertex_id} is not ok")
    overall_level = _level(payload, f"vertex {vertex_id} backpressure level")
    subtasks = payload.get("subtasks")
    if not isinstance(subtasks, list) or not subtasks:
        raise EvidenceError(f"backpressure response for vertex {vertex_id} has no subtasks")
    raw_subtasks: list[dict[str, Any]] = []
    for index, subtask in enumerate(subtasks):
        if not isinstance(subtask, dict):
            raise EvidenceError(f"vertex {vertex_id} subtasks[{index}] must be an object")
        subtask_number = subtask.get("subtask")
        if isinstance(subtask_number, bool) or not isinstance(subtask_number, int):
            raise EvidenceError(f"vertex {vertex_id} subtasks[{index}].subtask must be an integer")
        ratio = _number(subtask.get("ratio"), f"vertex {vertex_id} subtasks[{index}].ratio")
        if not 0.0 <= ratio <= 1.0:
            raise EvidenceError(f"vertex {vertex_id} subtasks[{index}].ratio must be in [0, 1]")
        raw: dict[str, Any] = {
            "subtask": subtask_number,
            "backpressure_level": _level(
                subtask,
                f"vertex {vertex_id} subtasks[{index}] backpressure level",
            ),
            "ratio": ratio,
        }
        attempt = subtask.get("attempt-number")
        if attempt is not None:
            if isinstance(attempt, bool) or not isinstance(attempt, int):
                raise EvidenceError(
                    f"vertex {vertex_id} subtasks[{index}].attempt-number must be an integer"
                )
            raw["attempt_number"] = attempt
        for source, destination in (("busyRatio", "busy_ratio"), ("idleRatio", "idle_ratio")):
            if source in subtask:
                value, unavailable_reason = _diagnostic_ratio(
                    subtask[source],
                    f"vertex {vertex_id} subtasks[{index}].{source}",
                )
                raw[destination] = value
                if unavailable_reason is not None:
                    raw[f"{destination}_unavailable"] = unavailable_reason
        raw_subtasks.append(raw)
    result: dict[str, Any] = {
        "vertex_id": vertex_id,
        "vertex_name": vertex_name,
        "backpressure_status": "ok",
        "backpressure_level": overall_level,
        "max_ratio": max(subtask["ratio"] for subtask in raw_subtasks),
        "subtasks": raw_subtasks,
    }
    end_timestamp = payload.get("end-timestamp")
    if end_timestamp is not None:
        if isinstance(end_timestamp, bool) or not isinstance(end_timestamp, int):
            raise EvidenceError(f"vertex {vertex_id} end-timestamp must be an integer")
        result["end_timestamp_ms"] = end_timestamp
    return result


def fetch_vertex_backpressure(
    client: DashboardSession,
    job_id: str,
    vertices: list[tuple[str, str]],
    *,
    timeout_seconds: float,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> list[dict[str, Any]]:
    """Resolve Flink's initial deprecated sampling response within one deadline."""
    timeout_seconds = _bounded_number(
        timeout_seconds,
        "backpressure_ready_timeout_seconds",
        1.0,
        MAX_BACKPRESSURE_READY_TIMEOUT_SECONDS,
    )
    deadline = monotonic() + timeout_seconds
    pending = list(vertices)
    observations: dict[str, dict[str, Any]] = {}
    attempt = 0
    while pending:
        if attempt > 0 and monotonic() >= deadline:
            vertex_ids = ",".join(vertex_id for vertex_id, _ in pending)
            raise EvidenceError(
                "Flink backpressure sampling did not become ready within "
                f"{timeout_seconds}s for vertices {vertex_ids}"
            )
        attempt += 1
        next_pending: list[tuple[str, str]] = []
        for vertex_id, name in pending:
            payload = client.get_json(f"/jobs/{job_id}/vertices/{vertex_id}/backpressure")
            if payload.get("status") == "deprecated":
                next_pending.append((vertex_id, name))
                continue
            observations[vertex_id] = parse_vertex_backpressure(payload, vertex_id, name)
        pending = next_pending
        if not pending:
            break
        remaining = deadline - monotonic()
        if remaining <= 0:
            vertex_ids = ",".join(vertex_id for vertex_id, _ in pending)
            raise EvidenceError(
                "Flink backpressure sampling did not become ready within "
                f"{timeout_seconds}s for vertices {vertex_ids}"
            )
        sleep(min(0.2, remaining))
    return [observations[vertex_id] for vertex_id, _ in vertices]


def collect_sample(
    client: DashboardSession,
    job_id: str,
    *,
    captured_at: datetime,
    seconds_from_load_start: float,
    backpressure_ready_timeout_seconds: float = 10.0,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Collect one job-state check plus every vertex's backpressure response."""
    job_id = _hex_id(job_id, "job_id")
    detail = client.get_json(f"/jobs/{job_id}")
    if _hex_id(detail.get("jid"), "job detail jid") != job_id:
        raise EvidenceError("job detail jid changed")
    if detail.get("state") != "RUNNING":
        raise EvidenceError(f"Flink job {job_id} is not RUNNING")
    vertices = detail.get("vertices")
    if not isinstance(vertices, list) or not vertices:
        raise EvidenceError(f"Flink job {job_id} has no vertices")

    vertex_names: list[tuple[str, str]] = []
    seen: set[str] = set()
    for index, vertex in enumerate(vertices):
        if not isinstance(vertex, dict):
            raise EvidenceError(f"vertices[{index}] must be an object")
        vertex_id = _hex_id(vertex.get("id"), f"vertices[{index}].id")
        if vertex_id in seen:
            raise EvidenceError(f"duplicate Flink vertex ID {vertex_id}")
        seen.add(vertex_id)
        name = vertex.get("name")
        if not isinstance(name, str) or not name:
            raise EvidenceError(f"vertices[{index}].name must be a nonempty string")
        vertex_names.append((vertex_id, name))
    observations = fetch_vertex_backpressure(
        client,
        job_id,
        vertex_names,
        timeout_seconds=backpressure_ready_timeout_seconds,
        monotonic=monotonic,
        sleep=sleep,
    )
    return {
        "captured_at": _utc_text(captured_at),
        "seconds_from_load_start": seconds_from_load_start,
        "job_id": job_id,
        "job_state": "RUNNING",
        "vertex_count": len(observations),
        "max_ratio": max(vertex["max_ratio"] for vertex in observations),
        "vertices": observations,
    }


def _interval_summary(
    samples: list[dict[str, Any]],
    predicate: Callable[[float], bool],
    interval: dict[str, Any],
) -> dict[str, Any]:
    selected = [sample for sample in samples if predicate(sample["seconds_from_load_start"])]
    per_vertex: dict[str, dict[str, Any]] = {}
    for sample in selected:
        for vertex in sample["vertices"]:
            current = per_vertex.setdefault(
                vertex["vertex_id"],
                {
                    "vertex_id": vertex["vertex_id"],
                    "vertex_name": vertex["vertex_name"],
                    "max_ratio": vertex["max_ratio"],
                },
            )
            current["max_ratio"] = max(current["max_ratio"], vertex["max_ratio"])
    return {
        "interval_seconds_from_load_start": interval,
        "sample_count": len(selected),
        "max_ratio": max((sample["max_ratio"] for sample in selected), default=None),
        "per_vertex": sorted(per_vertex.values(), key=lambda item: item["vertex_id"]),
    }


def _stable_tail(samples: list[dict[str, Any]]) -> dict[str, Any]:
    """Describe the final stable tail and the first point that stayed resolved."""
    tail = samples[-STABLE_TAIL_SAMPLE_COUNT:]
    tail_max = max(sample["max_ratio"] for sample in tail)
    first_resolved = None
    for index, candidate in enumerate(samples):
        remaining = samples[index:]
        if (
            len(remaining) >= STABLE_TAIL_SAMPLE_COUNT
            and max(sample["max_ratio"] for sample in remaining) <= 0.1
        ):
            first_resolved = candidate
            break
    return {
        "tail_sample_count": STABLE_TAIL_SAMPLE_COUNT,
        "tail_max_ratio": tail_max,
        "tail_start_seconds_from_load_start": tail[0]["seconds_from_load_start"],
        "tail_end_seconds_from_load_start": tail[-1]["seconds_from_load_start"],
        "first_resolved_at": (
            first_resolved["captured_at"] if first_resolved is not None else None
        ),
        "first_resolved_seconds_from_load_start": (
            first_resolved["seconds_from_load_start"] if first_resolved is not None else None
        ),
    }


def summarize_samples(
    samples: list[dict[str, Any]],
    load: LoadRun,
    capture_until_seconds: float | None = None,
) -> dict[str, Any]:
    """Separate baseline, burst, and recovery observations and evaluate thresholds."""
    if not samples:
        raise EvidenceError("no Flink backpressure samples were captured")
    capture_until_seconds = (
        load.duration_seconds
        if capture_until_seconds is None
        else _number(capture_until_seconds, "capture_until_seconds")
    )
    pre_burst_samples = [
        sample
        for sample in samples
        if 0.0 <= sample["seconds_from_load_start"] < load.burst_start_seconds
    ]
    pre_burst = _interval_summary(
        samples,
        lambda seconds: 0.0 <= seconds < load.burst_start_seconds,
        {"start_inclusive": 0.0, "end_exclusive": load.burst_start_seconds},
    )
    burst = _interval_summary(
        samples,
        lambda seconds: load.burst_start_seconds <= seconds <= load.burst_window_end_seconds,
        {
            "start_inclusive": load.burst_start_seconds,
            "end_inclusive": load.burst_window_end_seconds,
        },
    )
    observed_end_seconds = max(
        capture_until_seconds,
        max(sample["seconds_from_load_start"] for sample in samples),
    )
    recovery_samples = [
        sample
        for sample in samples
        if load.burst_window_end_seconds < sample["seconds_from_load_start"] <= observed_end_seconds
    ]
    recovery = _interval_summary(
        samples,
        lambda seconds: load.burst_window_end_seconds < seconds <= observed_end_seconds,
        {
            "start_exclusive": load.burst_window_end_seconds,
            "capture_target": capture_until_seconds,
            "observed_end_inclusive": observed_end_seconds,
        },
    )
    if pre_burst["sample_count"] == 0:
        raise EvidenceError("no samples were captured in the pre-burst interval")
    if len(pre_burst_samples) < STABLE_TAIL_SAMPLE_COUNT:
        raise EvidenceError("fewer than three samples were captured in the pre-burst interval")
    if burst["sample_count"] == 0:
        raise EvidenceError("no samples were captured in the burst interval")
    if recovery["sample_count"] == 0:
        raise EvidenceError("no samples were captured in the post-burst recovery interval")
    if len(recovery_samples) < STABLE_TAIL_SAMPLE_COUNT:
        raise EvidenceError(
            "fewer than three samples were captured in the post-burst recovery interval"
        )
    pre_burst.update(_stable_tail(pre_burst_samples))
    recovery.update(_stable_tail(recovery_samples))
    pre_condition = pre_burst["tail_max_ratio"] <= 0.1
    burst_condition = burst["max_ratio"] > 0.1
    recovery_condition = recovery["tail_max_ratio"] <= 0.1
    return {
        "pre_burst": pre_burst,
        "burst": burst,
        "post_burst_recovery": recovery,
        "evaluated_conditions": {
            "pre_burst_tail_max_ratio_lte_0_1": pre_condition,
            "burst_max_ratio_gt_0_1": burst_condition,
            "post_burst_tail_max_ratio_lte_0_1": recovery_condition,
            "all_conditions_true": pre_condition and burst_condition and recovery_condition,
        },
    }


def resolve_capture_settings(
    load: LoadRun,
    poll_interval_seconds: float,
    requested_duration_seconds: float | None,
    *,
    current_age_seconds: float,
    backpressure_ready_timeout_seconds: float = 10.0,
) -> CaptureSettings:
    """Resolve a finite timeline endpoint that covers the full burst window."""
    poll_interval_seconds = _bounded_number(
        poll_interval_seconds,
        "poll_interval_seconds",
        MIN_POLL_INTERVAL_SECONDS,
        MAX_POLL_INTERVAL_SECONDS,
    )
    capture_until = (
        load.duration_seconds
        if requested_duration_seconds is None
        else _bounded_number(
            requested_duration_seconds,
            "duration_seconds",
            MIN_POLL_INTERVAL_SECONDS,
            MAX_CAPTURE_DURATION_SECONDS,
        )
    )
    minimum_pre_burst_start = current_age_seconds + STABLE_TAIL_SAMPLE_COUNT * poll_interval_seconds
    if minimum_pre_burst_start > load.burst_start_seconds:
        raise EvidenceError("capture must start early enough for three pre-burst samples")
    minimum_capture_until = (
        load.burst_window_end_seconds + STABLE_TAIL_SAMPLE_COUNT * poll_interval_seconds
    )
    if capture_until < minimum_capture_until:
        raise EvidenceError(
            "duration_seconds must cover three post-burst recovery samples after "
            "burst_end_s + ramp_s"
        )
    if capture_until > load.duration_seconds:
        raise EvidenceError("duration_seconds must not exceed the load duration")
    if current_age_seconds >= capture_until:
        raise EvidenceError("capture duration already elapsed before sampling began")
    return CaptureSettings(
        poll_interval_seconds,
        capture_until,
        backpressure_ready_timeout_seconds,
    )


def capture_backpressure(
    client: DashboardSession,
    load: LoadRun,
    settings: CaptureSettings,
    *,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    utc_now: Callable[[], datetime] = lambda: datetime.now(UTC),
    on_progress: Callable[[str, list[dict[str, Any]]], None] | None = None,
) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
    """Poll until the requested point on the bound load timeline."""
    job_id = discover_running_job(client)
    bound_at = utc_now().astimezone(UTC)
    bound_monotonic = monotonic()
    age_at_bind = (bound_at - load.started_at).total_seconds()
    if age_at_bind < 0:
        raise EvidenceError("load start is in the future at capture bind")
    if age_at_bind >= load.burst_start_seconds:
        raise EvidenceError("capture bound too late to observe the pre-burst interval")
    if (
        age_at_bind + STABLE_TAIL_SAMPLE_COUNT * settings.poll_interval_seconds
        > load.burst_start_seconds
    ):
        raise EvidenceError("capture bound too late for three pre-burst samples")
    if age_at_bind >= settings.capture_until_seconds:
        raise EvidenceError("capture duration elapsed before the first sample")

    samples: list[dict[str, Any]] = []
    while True:
        now = utc_now().astimezone(UTC)
        seconds = age_at_bind + (monotonic() - bound_monotonic)
        sample = collect_sample(
            client,
            job_id,
            captured_at=now,
            seconds_from_load_start=seconds,
            backpressure_ready_timeout_seconds=(settings.backpressure_ready_timeout_seconds),
            monotonic=monotonic,
            sleep=sleep,
        )
        # A deprecated response can delay the usable sample. Record when every
        # vertex actually returned status=ok, not when the first GET began.
        sample["captured_at"] = _utc_text(utc_now().astimezone(UTC))
        sample["seconds_from_load_start"] = age_at_bind + (monotonic() - bound_monotonic)
        samples.append(sample)
        if on_progress is not None:
            on_progress(job_id, samples)
        seconds = sample["seconds_from_load_start"]
        if seconds >= settings.capture_until_seconds:
            break
        remaining = settings.capture_until_seconds - seconds
        sleep(min(settings.poll_interval_seconds, remaining))
    return (
        job_id,
        samples,
        summarize_samples(
            samples,
            load,
            settings.capture_until_seconds,
        ),
    )


def claim_artifact(path: Path, payload: dict[str, Any]) -> None:
    """Claim a one-use evidence path without overwriting prior observations."""
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".claim", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
        # A same-directory hard link atomically publishes the complete file and
        # fails if prior evidence already owns the target path.
        os.link(temporary, path)
    except Exception:
        raise
    finally:
        temporary.unlink(missing_ok=True)


def write_artifact(path: Path, payload: dict[str, Any]) -> None:
    """Atomically replace the current running, completed, or failed evidence."""
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
        temporary.replace(path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Capture Flink 1.20 backpressure evidence for one W4 load run."
    )
    parser.add_argument(
        "--dashboard-url",
        default=os.environ.get("FLINK_DASHBOARD_URL"),
        help="Human-generated FLINK_DASHBOARD_URL presigned authorization URL.",
    )
    parser.add_argument(
        "--load-artifact",
        default=os.environ.get("W4_ARTIFACT_PATH"),
        help="Path to the bound kinesis-load.json artifact (default: W4_ARTIFACT_PATH).",
    )
    parser.add_argument(
        "--expected-run-id",
        default=os.environ.get("W4_RUN_ID"),
        help="Canonical UUID4 expected in the load artifact (default: W4_RUN_ID).",
    )
    parser.add_argument(
        "--artifact",
        default=os.environ.get("W4_FLINK_BACKPRESSURE_ARTIFACT_PATH"),
        help="Fresh output path (default: W4_FLINK_BACKPRESSURE_ARTIFACT_PATH).",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=os.environ.get("FLINK_BACKPRESSURE_POLL_INTERVAL_S", "2"),
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=os.environ.get("FLINK_BACKPRESSURE_DURATION_S"),
        help="Capture-until offset from load start; defaults to the load duration.",
    )
    parser.add_argument(
        "--request-timeout-seconds",
        type=float,
        default=os.environ.get("FLINK_DASHBOARD_REQUEST_TIMEOUT_S", "10"),
    )
    parser.add_argument(
        "--backpressure-ready-timeout-seconds",
        type=float,
        default=os.environ.get("FLINK_BACKPRESSURE_READY_TIMEOUT_S", "10"),
        help="Bounded wait for an initial Flink status=deprecated response.",
    )
    parser.add_argument(
        "--load-wait-timeout-seconds",
        type=float,
        default=os.environ.get("FLINK_LOAD_WAIT_TIMEOUT_S", "120"),
    )
    parser.add_argument(
        "--max-load-start-age-seconds",
        type=float,
        default=os.environ.get("FLINK_MAX_LOAD_START_AGE_S", "120"),
    )
    return parser


def _required_args(args: argparse.Namespace) -> None:
    for attribute, label in (
        ("dashboard_url", "--dashboard-url or FLINK_DASHBOARD_URL"),
        ("load_artifact", "--load-artifact or W4_ARTIFACT_PATH"),
        ("expected_run_id", "--expected-run-id or W4_RUN_ID"),
        ("artifact", "--artifact or W4_FLINK_BACKPRESSURE_ARTIFACT_PATH"),
    ):
        if not getattr(args, attribute):
            raise EvidenceError(f"{label} is required")


def execute(args: argparse.Namespace) -> int:
    """Run one capture and leave a one-use atomic evidence artifact."""
    _required_args(args)
    expected_run_id = validate_run_id(args.expected_run_id)
    authorization_url = validate_authorization_url(args.dashboard_url)
    request_timeout = _bounded_number(
        args.request_timeout_seconds,
        "request_timeout_seconds",
        1.0,
        MAX_REQUEST_TIMEOUT_SECONDS,
    )
    backpressure_ready_timeout = _bounded_number(
        args.backpressure_ready_timeout_seconds,
        "backpressure_ready_timeout_seconds",
        1.0,
        MAX_BACKPRESSURE_READY_TIMEOUT_SECONDS,
    )
    wait_timeout = _bounded_number(
        args.load_wait_timeout_seconds,
        "load_wait_timeout_seconds",
        1.0,
        MAX_WAIT_TIMEOUT_SECONDS,
    )
    max_start_age = _bounded_number(
        args.max_load_start_age_seconds,
        "max_load_start_age_seconds",
        1.0,
        MAX_WAIT_TIMEOUT_SECONDS,
    )
    _bounded_number(
        args.poll_interval_seconds,
        "poll_interval_seconds",
        MIN_POLL_INTERVAL_SECONDS,
        MAX_POLL_INTERVAL_SECONDS,
    )
    if args.duration_seconds is not None:
        _bounded_number(
            args.duration_seconds,
            "duration_seconds",
            MIN_POLL_INTERVAL_SECONDS,
            MAX_CAPTURE_DURATION_SECONDS,
        )

    load_path = Path(args.load_artifact).expanduser().resolve()
    artifact_path = Path(args.artifact).expanduser().resolve()
    if load_path == artifact_path:
        raise EvidenceError("output artifact must differ from the load artifact")
    started_at = datetime.now(UTC)
    base: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "status": "running",
        "phase": "authorizing_dashboard",
        "run_id": expected_run_id,
        "load_artifact": str(load_path),
        "artifact": str(artifact_path),
        "started_at": _utc_text(started_at),
        "sample_count": 0,
        "samples": [],
    }
    try:
        claim_artifact(artifact_path, base)
    except FileExistsError as error:
        raise EvidenceError(f"output artifact already exists: {artifact_path}") from error
    except OSError as error:
        raise EvidenceError(
            f"unable to atomically claim output artifact: {artifact_path}"
        ) from error

    latest_samples: list[dict[str, Any]] = []
    try:
        client = DashboardSession.authorize(authorization_url, request_timeout)
        base.update(
            phase="waiting_for_load",
            dashboard_origin=client.origin,
            dashboard_api_base_path=client.api_base_path,
        )
        write_artifact(artifact_path, base)
        load = wait_for_fresh_load(
            load_path,
            expected_run_id,
            timeout_seconds=wait_timeout,
            max_start_age_seconds=max_start_age,
        )
        now = datetime.now(UTC)
        load_age = (now - load.started_at).total_seconds()
        settings = resolve_capture_settings(
            load,
            args.poll_interval_seconds,
            args.duration_seconds,
            current_age_seconds=load_age,
            backpressure_ready_timeout_seconds=backpressure_ready_timeout,
        )
        base.update(
            phase="capturing",
            load_started_at=_utc_text(load.started_at),
            load_duration_seconds=load.duration_seconds,
            load_profile=load.profile,
            poll_interval_seconds=settings.poll_interval_seconds,
            backpressure_ready_timeout_seconds=(settings.backpressure_ready_timeout_seconds),
            capture_until_seconds_from_load_start=settings.capture_until_seconds,
        )
        write_artifact(artifact_path, base)

        def progress(job_id: str, samples: list[dict[str, Any]]) -> None:
            nonlocal latest_samples
            latest_samples = samples
            write_artifact(
                artifact_path,
                {
                    **base,
                    "job_id": job_id,
                    "sample_count": len(samples),
                    "samples": samples,
                },
            )

        job_id, samples, summary = capture_backpressure(
            client,
            load,
            settings,
            on_progress=progress,
        )
        completed = {
            **base,
            "status": "completed",
            "phase": "completed",
            "ended_at": _utc_text(datetime.now(UTC)),
            "job_id": job_id,
            "sample_count": len(samples),
            "samples": samples,
            "summary": summary,
        }
        write_artifact(artifact_path, completed)
    except (Exception, KeyboardInterrupt) as error:
        failed = {
            **base,
            "status": "failed",
            "phase": "failed",
            "ended_at": _utc_text(datetime.now(UTC)),
            "sample_count": len(latest_samples),
            "samples": latest_samples,
            "error": f"{type(error).__name__}: {error}",
        }
        write_artifact(artifact_path, failed)
        print(str(error), file=sys.stderr)
        return 1
    print(f"evidence artifact: {artifact_path}", flush=True)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return execute(args)
    except EvidenceError as error:
        print(str(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
