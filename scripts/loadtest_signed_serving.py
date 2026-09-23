#!/usr/bin/env python3
"""Run a bounded, signed Wave 5 serving load or soak trial.

This tool is authored for a future operator-run AWS window. It does nothing unless
the operator supplies every live control plus the exact confirmation phrase.
There is no automatic retry: one scheduled request means at most one signed
HTTP attempt, so request counts and later cost reconciliation stay honest.

The raw event ledger is append-only. Warmup traffic is retained in that ledger
but excluded from measured trial summaries. Client-observed latency and the
server-reported ``latency_ms`` field are always reported separately.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import secrets
import signal
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import Counter
from collections.abc import Callable, Iterable, MutableMapping
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, BinaryIO, Protocol

REPO_ROOT = Path(__file__).resolve().parents[1]
for _path in (REPO_ROOT, REPO_ROOT / "serving", REPO_ROOT / "streaming"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from app.models import AisScoreIn  # noqa: E402
from flink.window_logic import (  # noqa: E402
    open_no_redirect,
    sigv4_headers,
    validate_ais_score_response,
    validate_execute_api_url,
)

SCHEMA_VERSION = 1
CONFIRM_PHRASE = "RUN SIGNED SERVING LOAD"
WORKLOAD_ID = "serving-fixed-v1"
PERCENTILE_METHOD = "R-7 linear interpolation at q*(n-1)"

MAX_TARGET_RPS = 50.0
MAX_IN_FLIGHT = 100
MAX_ACTIVE_SECONDS = 4 * 60 * 60
MAX_TOTAL_REQUESTS = 100_000
MAX_REQUEST_TIMEOUT_SECONDS = 30.0
MAX_RESPONSE_BYTES = 64 * 1024
MAX_EVENT_LEDGER_BYTES = 128 * 1024 * 1024
READONLY_GUARD_RESERVE_SECONDS = 60.0
EVIDENCE_SEAL_RESERVE_SECONDS = 30.0
POST_KILL_REAP_SECONDS = 5.0
SUPERVISOR_INTERRUPT_POLL_SECONDS = 0.1
# The operator supplies the reviewed account through HM_LOADTEST_ACCOUNT_ID, so
# no real account ID is written into the repository. The default is the AWS
# documentation placeholder, which never matches a real caller, so the preflight
# ownership check refuses to run until the operator sets the variable.
EXPECTED_ACCOUNT_ID = os.environ.get("HM_LOADTEST_ACCOUNT_ID", "123456789012")
if re.fullmatch(r"[0-9]{12}", EXPECTED_ACCOUNT_ID) is None:
    raise SystemExit("HM_LOADTEST_ACCOUNT_ID must be exactly 12 digits")
EXPECTED_REGION = "us-east-1"
EXPECTED_API_NAME = "harbormaster-base-serving-api"
EXPECTED_SCORE_PATH = "/v1/score-ais"
# Pinned to aws_apigatewayv2_route.proxy.route_key in
# infra/terraform/modules/apigw/main.tf. A regression test keeps the two exact.
EXPECTED_ROUTE_KEY = "ANY /{proxy+}"
EXPECTED_ROUTE_AUTHORIZATION = "AWS_IAM"
EXPECTED_INTEGRATION_TYPE = "HTTP_PROXY"
EXPECTED_INTEGRATION_METHOD = "ANY"
EXPECTED_CONNECTION_TYPE = "VPC_LINK"
OFFICIAL_STS_ENDPOINT = "https://sts.us-east-1.amazonaws.com"
OFFICIAL_APIGATEWAYV2_ENDPOINT = "https://apigateway.us-east-1.amazonaws.com"
MAX_ROUTE_PAGES = 20
EXPECTED_WORKLOAD_SHA256 = "ee96802abc620781276b1b2a06a7ff3deba145c59203396182caa3524b1d05c0"
PLATFORM_CALLER_RE = re.compile(
    rf"^arn:aws:sts::{re.escape(EXPECTED_ACCOUNT_ID)}:assumed-role/harbormaster-platform/"
    r"[A-Za-z0-9+=,.@_-]+$"
)
HEX_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
TRACE_ID_RE = re.compile(r"^[0-9a-f]{32}$")

Transport = Callable[[str, str, bytes, float], "RawHttpResult"]


class Executor(Protocol):
    def submit(self, fn: Callable[..., dict[str, Any]], /, *args: Any) -> Future: ...

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None: ...


@dataclass(frozen=True)
class Workload:
    identifier: str
    body: bytes
    mmsi: int
    sha256: str


@dataclass(frozen=True)
class RawHttpResult:
    status_code: int | None
    body: bytes
    error_code: str | None = None


@dataclass(frozen=True)
class Phase:
    name: str
    trial_index: int | None
    duration_seconds: float


@dataclass(frozen=True)
class ScheduledRequest:
    request_ordinal: int
    phase: str
    trial_index: int | None
    phase_request_ordinal: int
    scheduled_offset_seconds: float


@dataclass(frozen=True)
class SupervisorPlan:
    cutoff_utc: datetime
    started_utc: datetime
    started_monotonic: float
    graceful_stop_monotonic: float
    hard_stop_monotonic: float
    required_window_seconds: float
    termination_grace_seconds: float


@dataclass(frozen=True)
class SupervisorClaim:
    claim_id: str
    claim_path: Path
    claim_manifest_path: Path
    claim_sha256: str


@dataclass(frozen=True)
class SupervisorGrant:
    schema_version: int
    nonce: str
    parent_pid: int
    run_id: str
    expected_api_id: str
    expected_integration_id: str
    expected_integration_uri: str
    expected_git_head: str
    expected_harness_sha256: str
    expected_window_logic_sha256: str
    expected_models_sha256: str
    artifact_dir: str
    operator_cutoff_utc: str
    claim_id: str
    claim_filename: str
    claim_sha256: str


_ACTIVE_SUPERVISOR_GRANT: SupervisorGrant | None = None


@dataclass(frozen=True)
class RunConfig:
    kind: str
    run_id: str
    api_url: str
    region: str
    expected_api_id: str
    expected_integration_id: str
    expected_integration_uri: str
    expected_git_head: str
    expected_harness_sha256: str
    expected_window_logic_sha256: str
    expected_models_sha256: str
    operator_cutoff_utc: str
    artifact_dir: Path
    target_rps: float
    max_in_flight: int
    warmup_seconds: float
    trial_seconds: float
    trials: int
    cooldown_seconds: float
    request_timeout_seconds: float
    drain_timeout_seconds: float
    goodput_max_scheduled_response_ms: float
    max_schedule_lag_ms: float
    max_total_requests: int
    minimum_success_ratio: float
    minimum_goodput_ratio: float

    def __post_init__(self) -> None:
        if self.kind not in {"load", "soak"}:
            raise ValueError("kind must be load or soak")
        if self.region != EXPECTED_REGION:
            raise ValueError(f"region must equal {EXPECTED_REGION}")
        validate_run_id(self.run_id)
        validate_execute_api_url(self.api_url, self.region)
        parsed_host = urllib.parse.urlsplit(self.api_url).hostname or ""
        actual_api_id = parsed_host.split(".", 1)[0]
        if not re.fullmatch(r"[a-z0-9]{6,32}", self.expected_api_id):
            raise ValueError("expected_api_id must be a lowercase API Gateway API ID")
        if actual_api_id != self.expected_api_id:
            raise ValueError("expected_api_id does not match api_url")
        if urllib.parse.urlsplit(self.api_url).path != EXPECTED_SCORE_PATH:
            raise ValueError(f"api_url path must equal {EXPECTED_SCORE_PATH}")
        if not re.fullmatch(r"[a-z0-9]{6,32}", self.expected_integration_id):
            raise ValueError(
                "expected_integration_id must be a lowercase API Gateway integration ID"
            )
        _validate_expected_integration_uri(self.expected_integration_uri)
        if not GIT_SHA_RE.fullmatch(self.expected_git_head):
            raise ValueError("expected_git_head must be a full lowercase Git SHA")
        for name, value in (
            ("expected_harness_sha256", self.expected_harness_sha256),
            ("expected_window_logic_sha256", self.expected_window_logic_sha256),
            ("expected_models_sha256", self.expected_models_sha256),
        ):
            if not HEX_SHA256_RE.fullmatch(value):
                raise ValueError(f"{name} must be a lowercase SHA256")
        parse_utc_cutoff(self.operator_cutoff_utc)
        _bounded_float(self.target_rps, "target_rps", lower=0, upper=MAX_TARGET_RPS)
        _bounded_int(self.max_in_flight, "max_in_flight", lower=1, upper=MAX_IN_FLIGHT)
        _bounded_float(
            self.warmup_seconds,
            "warmup_seconds",
            lower=0,
            upper=MAX_ACTIVE_SECONDS,
            lower_inclusive=True,
        )
        _bounded_float(
            self.trial_seconds,
            "trial_seconds",
            lower=0,
            upper=MAX_ACTIVE_SECONDS,
        )
        _bounded_float(
            self.cooldown_seconds,
            "cooldown_seconds",
            lower=0,
            upper=MAX_ACTIVE_SECONDS,
            lower_inclusive=True,
        )
        _bounded_float(
            self.request_timeout_seconds,
            "request_timeout_seconds",
            lower=0,
            upper=MAX_REQUEST_TIMEOUT_SECONDS,
        )
        _bounded_float(
            self.drain_timeout_seconds,
            "drain_timeout_seconds",
            lower=self.request_timeout_seconds,
            upper=MAX_REQUEST_TIMEOUT_SECONDS + 30,
            lower_inclusive=True,
        )
        _bounded_float(
            self.goodput_max_scheduled_response_ms,
            "goodput_max_scheduled_response_ms",
            lower=0,
            upper=60_000,
        )
        _bounded_float(
            self.max_schedule_lag_ms,
            "max_schedule_lag_ms",
            lower=0,
            upper=60_000,
        )
        _bounded_int(
            self.max_total_requests, "max_total_requests", lower=1, upper=MAX_TOTAL_REQUESTS
        )
        _ratio(self.minimum_success_ratio, "minimum_success_ratio")
        _ratio(self.minimum_goodput_ratio, "minimum_goodput_ratio")

        if self.kind == "load" and self.trials < 3:
            raise ValueError("load kind requires at least 3 measured trials")
        if self.kind == "soak" and self.trials != 1:
            raise ValueError("soak kind requires exactly 1 measured trial")
        if self.kind == "soak" and self.trial_seconds < 3600:
            raise ValueError("soak kind requires trial_seconds >= 3600")
        _bounded_int(self.trials, "trials", lower=1, upper=20)

        active_seconds = self.active_phase_seconds
        if self.required_supervised_window_seconds > MAX_ACTIVE_SECONDS:
            raise ValueError(
                "configured guards, phases, drains, termination, and sealing exceed "
                f"{MAX_ACTIVE_SECONDS} seconds"
            )
        if active_seconds > MAX_ACTIVE_SECONDS:
            raise ValueError(f"configured active time exceeds {MAX_ACTIVE_SECONDS} seconds")

        if self.measurement_requests_per_trial < 1:
            raise ValueError("each measured trial must schedule at least one request")
        if self.planned_total_client_requests > self.max_total_requests:
            raise ValueError(
                "planned client request count, including preflight, exceeds "
                "max_total_requests: "
                f"{self.planned_total_client_requests} > {self.max_total_requests}"
            )

    @property
    def active_phase_seconds(self) -> float:
        return (
            self.warmup_seconds
            + self.trials * self.trial_seconds
            + max(0, self.trials - 1) * self.cooldown_seconds
        )

    @property
    def request_phase_count(self) -> int:
        return self.trials + int(self.warmup_seconds > 0)

    @property
    def required_supervised_window_seconds(self) -> float:
        """One reviewed equation for feasibility and supervisor deadlines."""
        return (
            READONLY_GUARD_RESERVE_SECONDS
            + self.request_timeout_seconds
            + self.active_phase_seconds
            + self.request_phase_count * self.drain_timeout_seconds
            + EVIDENCE_SEAL_RESERVE_SECONDS
            + self.termination_grace_seconds
        )

    @property
    def termination_grace_seconds(self) -> float:
        return self.request_timeout_seconds

    @property
    def warmup_request_count(self) -> int:
        return request_count(self.warmup_seconds, self.target_rps)

    @property
    def measurement_requests_per_trial(self) -> int:
        return request_count(self.trial_seconds, self.target_rps)

    @property
    def planned_request_count(self) -> int:
        return self.warmup_request_count + self.trials * self.measurement_requests_per_trial

    @property
    def planned_total_client_requests(self) -> int:
        """Maximum transport attempts, including the one required preflight."""
        return 1 + self.planned_request_count

    def phases(self) -> list[Phase]:
        phases: list[Phase] = []
        if self.warmup_seconds > 0:
            phases.append(Phase("warmup", None, self.warmup_seconds))
        for trial_index in range(1, self.trials + 1):
            phases.append(Phase("trial", trial_index, self.trial_seconds))
            if trial_index < self.trials and self.cooldown_seconds > 0:
                phases.append(Phase("cooldown", trial_index, self.cooldown_seconds))
        return phases


def _validate_expected_integration_uri(value: str) -> str:
    """Require one reviewed regional integration ARN owned by this account."""
    if not isinstance(value, str) or any(character.isspace() for character in value):
        raise ValueError("expected_integration_uri must be one integration ARN")
    parts = value.split(":", 5)
    if len(parts) != 6:
        raise ValueError("expected_integration_uri must be one integration ARN")
    arn, partition, service, region, account, resource = parts
    if (
        arn != "arn"
        or partition != "aws"
        or region != EXPECTED_REGION
        or account != EXPECTED_ACCOUNT_ID
    ):
        raise ValueError("expected_integration_uri must belong to the reviewed account and region")
    if service == "servicediscovery":
        valid_resource = re.fullmatch(r"service/srv-[A-Za-z0-9]+", resource) is not None
    elif service == "elasticloadbalancing":
        valid_resource = (
            re.fullmatch(
                r"listener/(?:app|net)/[A-Za-z0-9-]{1,32}/[0-9a-f]+/[0-9a-f]+",
                resource,
            )
            is not None
        )
    else:
        valid_resource = False
    if not valid_resource:
        raise ValueError(
            "expected_integration_uri must be a Cloud Map service or load balancer listener ARN"
        )
    return value


def _bounded_float(
    value: float,
    name: str,
    *,
    lower: float,
    upper: float,
    lower_inclusive: bool = False,
) -> float:
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    below = value < lower if lower_inclusive else value <= lower
    if below or value > upper:
        relation = ">=" if lower_inclusive else ">"
        raise ValueError(f"{name} must be {relation} {lower} and <= {upper}")
    return value


def _bounded_int(value: int, name: str, *, lower: int, upper: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not lower <= value <= upper:
        raise ValueError(f"{name} must be an integer within [{lower}, {upper}]")
    return value


def _ratio(value: float, name: str) -> float:
    if not math.isfinite(value) or not 0 <= value <= 1:
        raise ValueError(f"{name} must be finite and within [0, 1]")
    return value


def validate_run_id(value: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError("run_id must be a canonical UUID4") from error
    if parsed.version != 4 or str(parsed) != value:
        raise ValueError("run_id must be a canonical UUID4")
    return value


def parse_utc_cutoff(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("operator_cutoff_utc must be an ISO-8601 UTC timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("operator_cutoff_utc must include the UTC offset")
    return parsed.astimezone(UTC)


def request_count(duration_seconds: float, target_rps: float) -> int:
    """Return floor(duration * rate), with a small binary-rounding guard."""
    return math.floor(duration_seconds * target_rps + 1e-12)


def schedule_phase(
    phase: Phase,
    target_rps: float,
    *,
    first_request_ordinal: int,
) -> list[ScheduledRequest]:
    count = 0 if phase.name == "cooldown" else request_count(phase.duration_seconds, target_rps)
    return [
        ScheduledRequest(
            request_ordinal=first_request_ordinal + index,
            phase=phase.name,
            trial_index=phase.trial_index,
            phase_request_ordinal=index + 1,
            scheduled_offset_seconds=index / target_rps,
        )
        for index in range(count)
    ]


def load_workload(path: Path) -> Workload:
    raw = path.expanduser().resolve().read_bytes()
    if len(raw) > 64 * 1024:
        raise ValueError("workload JSON must be <= 65536 bytes")
    try:
        payload = json.loads(raw)
        parsed = AisScoreIn.model_validate(payload)
    except Exception as error:
        raise ValueError("workload must be one valid AisScoreIn JSON object") from error
    canonical = json.dumps(
        parsed.model_dump(mode="json", exclude_none=True),
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    canonical_sha256 = hashlib.sha256(canonical).hexdigest()
    if canonical_sha256 != EXPECTED_WORKLOAD_SHA256:
        raise ValueError(
            f"workload does not match the pinned wave5_score_request fixture: {canonical_sha256}"
        )
    return Workload(
        identifier=WORKLOAD_ID,
        body=canonical,
        mmsi=parsed.mmsi,
        sha256=canonical_sha256,
    )


def _read_bounded(response: Any) -> tuple[bytes, str | None]:
    body = response.read(MAX_RESPONSE_BYTES + 1)
    if len(body) > MAX_RESPONSE_BYTES:
        return b"", "response_too_large"
    return body, None


def signed_http_request(
    url: str,
    region: str,
    body: bytes,
    timeout_seconds: float,
    *,
    supervisor_grant: SupervisorGrant | None = None,
) -> RawHttpResult:
    """Send one freshly signed request under the active inherited grant."""
    if supervisor_grant is None or supervisor_grant is not _ACTIVE_SUPERVISOR_GRANT:
        raise RuntimeError("signed transport requires the active supervisor grant")
    headers = sigv4_headers(url, body, region)
    request = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with open_no_redirect(request, timeout_seconds=timeout_seconds) as response:
            response_body, error_code = _read_bounded(response)
            return RawHttpResult(int(response.status), response_body, error_code)
    except urllib.error.HTTPError as error:
        try:
            response_body, error_code = _read_bounded(error)
        finally:
            error.close()
        return RawHttpResult(int(error.code), response_body, error_code)
    except (TimeoutError, urllib.error.URLError) as error:
        reason = getattr(error, "reason", None)
        code = (
            "timeout"
            if isinstance(error, TimeoutError) or isinstance(reason, TimeoutError)
            else "url_error"
        )
        return RawHttpResult(None, b"", code)
    except OSError:
        return RawHttpResult(None, b"", "os_error")


def _response_observation(payload: dict[str, Any]) -> dict[str, Any]:
    trace_id = str(payload["trace_id"])
    if not TRACE_ID_RE.fullmatch(trace_id):
        raise ValueError("scorer response trace_id must be 32 lowercase hexadecimal characters")
    reasons = payload["reasons"]
    if len(reasons) > 32:
        raise ValueError("scorer response has too many reasons")
    reason_codes = [str(reason["code"]) for reason in reasons]
    if any(not re.fullmatch(r"[a-z0-9_]{1,64}", code) for code in reason_codes):
        raise ValueError("scorer response reason code format is invalid")
    return {
        "score": float(payload["score"]),
        "confidence": float(payload["confidence"]),
        "reason_codes": reason_codes,
        "hitl_required": bool(payload["hitl_required"]),
        "trace_id": trace_id,
        "server_latency_ms": float(payload["latency_ms"]),
        "returned_mmsi": int(payload["mmsi"]),
        "n_history": int(payload["n_history"]),
    }


def _safe_boring_response(observation: dict[str, Any]) -> bool:
    return observation["hitl_required"] is False and observation["reason_codes"] == []


def _utc_z(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _request_record(
    scheduled: ScheduledRequest,
    *,
    request_id: str,
    phase_started_monotonic: float,
    phase_end_monotonic: float,
    config: RunConfig,
    workload: Workload,
    transport: Transport,
    monotonic: Callable[[], float],
    utc_now: Callable[[], datetime],
    stop_event: threading.Event,
) -> dict[str, Any]:
    deadline = phase_started_monotonic + scheduled.scheduled_offset_seconds
    started = monotonic()
    schedule_lag_ms = max(0.0, (started - deadline) * 1000.0)
    if stop_event.is_set():
        return _dropped_request_record(
            scheduled,
            request_id=request_id,
            outcome="stop_requested_drop",
            schedule_lag_ms=schedule_lag_ms,
            workload=workload,
        )
    if schedule_lag_ms > config.max_schedule_lag_ms or started >= phase_end_monotonic:
        return _dropped_request_record(
            scheduled,
            request_id=request_id,
            outcome="schedule_lag_drop",
            schedule_lag_ms=schedule_lag_ms,
            workload=workload,
        )
    started_at = utc_now()
    try:
        raw = transport(
            config.api_url,
            config.region,
            workload.body,
            config.request_timeout_seconds,
        )
    except Exception:
        raw = RawHttpResult(None, b"", "transport_exception")
    ended = monotonic()
    ended_at = utc_now()

    client_latency_ms = max(0.0, (ended - started) * 1000.0)
    scheduled_to_completion_ms = max(0.0, (ended - deadline) * 1000.0)
    outcome = "transport_error"
    error_code = raw.error_code
    returned_mmsi: int | None = None
    server_latency_ms: float | None = None
    trace_id: str | None = None
    response_contract_valid = False
    response_score: float | None = None
    response_confidence: float | None = None
    response_reason_codes: list[str] = []
    response_hitl_required: bool | None = None
    response_n_history: int | None = None

    if raw.error_code is None and raw.status_code == 200:
        try:
            payload = validate_ais_score_response(raw.status_code, raw.body, workload.mmsi)
            observation = _response_observation(payload)
        except ValueError:
            outcome = "contract_error"
            error_code = "invalid_ais_score_response"
        else:
            response_contract_valid = True
            returned_mmsi = observation["returned_mmsi"]
            server_latency_ms = observation["server_latency_ms"]
            trace_id = observation["trace_id"]
            response_score = observation["score"]
            response_confidence = observation["confidence"]
            response_reason_codes = observation["reason_codes"]
            response_hitl_required = observation["hitl_required"]
            response_n_history = observation["n_history"]
            if _safe_boring_response(observation):
                outcome = "success"
                error_code = None
            else:
                outcome = "unsafe_stateful_response"
                error_code = "response_outside_boring_non_hitl_envelope"
                stop_event.set()
    elif raw.error_code is not None:
        outcome = "transport_error"
    elif raw.status_code is not None and 400 <= raw.status_code <= 499:
        outcome = "http_4xx"
        error_code = f"http_{raw.status_code}"
    elif raw.status_code is not None and 500 <= raw.status_code <= 599:
        outcome = "http_5xx"
        error_code = f"http_{raw.status_code}"
    else:
        outcome = "http_other"
        error_code = f"http_{raw.status_code}"

    return {
        "request_id": request_id,
        "request_ordinal": scheduled.request_ordinal,
        "phase": scheduled.phase,
        "trial_index": scheduled.trial_index,
        "phase_request_ordinal": scheduled.phase_request_ordinal,
        "scheduled_offset_seconds": scheduled.scheduled_offset_seconds,
        "started_offset_seconds": max(0.0, started - phase_started_monotonic),
        "ended_offset_seconds": max(0.0, ended - phase_started_monotonic),
        "started_at_utc": _utc_z(started_at),
        "ended_at_utc": _utc_z(ended_at),
        "schedule_lag_ms": schedule_lag_ms,
        "client_latency_ms": client_latency_ms,
        "scheduled_to_completion_ms": scheduled_to_completion_ms,
        "http_status": raw.status_code,
        "outcome": outcome,
        "error_code": error_code,
        "response_contract_valid": response_contract_valid,
        "expected_mmsi": workload.mmsi,
        "returned_mmsi": returned_mmsi,
        "server_latency_ms": server_latency_ms,
        "trace_id": trace_id,
        "response_score": response_score,
        "response_confidence": response_confidence,
        "response_reason_codes": response_reason_codes,
        "response_hitl_required": response_hitl_required,
        "response_n_history": response_n_history,
        "good": (
            outcome == "success"
            and scheduled_to_completion_ms <= config.goodput_max_scheduled_response_ms
        ),
    }


def _dropped_request_record(
    scheduled: ScheduledRequest,
    *,
    request_id: str,
    outcome: str,
    schedule_lag_ms: float,
    workload: Workload,
) -> dict[str, Any]:
    return {
        "request_id": request_id,
        "request_ordinal": scheduled.request_ordinal,
        "phase": scheduled.phase,
        "trial_index": scheduled.trial_index,
        "phase_request_ordinal": scheduled.phase_request_ordinal,
        "scheduled_offset_seconds": scheduled.scheduled_offset_seconds,
        "started_offset_seconds": None,
        "ended_offset_seconds": None,
        "started_at_utc": None,
        "ended_at_utc": None,
        "schedule_lag_ms": schedule_lag_ms,
        "client_latency_ms": None,
        "scheduled_to_completion_ms": None,
        "http_status": None,
        "outcome": outcome,
        "error_code": outcome,
        "response_contract_valid": False,
        "expected_mmsi": workload.mmsi,
        "returned_mmsi": None,
        "server_latency_ms": None,
        "trace_id": None,
        "response_score": None,
        "response_confidence": None,
        "response_reason_codes": [],
        "response_hitl_required": None,
        "response_n_history": None,
        "good": False,
    }


class EventLedger:
    def __init__(
        self,
        path: Path,
        run_id: str,
        *,
        max_bytes: int = MAX_EVENT_LEDGER_BYTES,
    ) -> None:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        self._handle = os.fdopen(descriptor, "w", encoding="utf-8")
        self._run_id = run_id
        self._event_seq = 0
        self._bytes_written = 0
        self._max_bytes = max_bytes
        self._lock = threading.Lock()
        self._closed = False

    def write(self, record_type: str, **fields: Any) -> dict[str, Any]:
        with self._lock:
            if self._closed:
                raise RuntimeError("event ledger is closed")
            next_event_seq = self._event_seq + 1
            record = {
                "schema_version": SCHEMA_VERSION,
                "run_id": self._run_id,
                "event_seq": next_event_seq,
                "record_type": record_type,
                **fields,
            }
            line = json.dumps(
                record,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            encoded_bytes = len(line.encode("utf-8")) + 1
            if self._bytes_written + encoded_bytes > self._max_bytes:
                raise RuntimeError("event ledger byte ceiling exceeded")
            self._handle.write(line + "\n")
            self._handle.flush()
            self._event_seq = next_event_seq
            self._bytes_written += encoded_bytes
            return record

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._handle.flush()
            os.fsync(self._handle.fileno())
            self._handle.close()
            self._closed = True


def _sleep_until(
    deadline: float,
    *,
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
    stop_event: threading.Event | None = None,
) -> bool:
    remaining = deadline - monotonic()
    if remaining > 0:
        if stop_event is not None and sleep is time.sleep:
            stop_event.wait(remaining)
        else:
            sleep(remaining)
    return stop_event is None or not stop_event.is_set()


def _emit_request(
    ledger: EventLedger,
    records: list[dict[str, Any]],
    records_lock: threading.Lock,
    record: dict[str, Any],
) -> dict[str, Any]:
    event = ledger.write("request", **record)
    with records_lock:
        records.append(event)
    return event


def _execute_request_and_emit(
    scheduled: ScheduledRequest,
    *,
    request_id: str,
    phase_started_monotonic: float,
    phase_end_monotonic: float,
    config: RunConfig,
    workload: Workload,
    transport: Transport,
    monotonic: Callable[[], float],
    utc_now: Callable[[], datetime],
    ledger: EventLedger,
    records: list[dict[str, Any]],
    records_lock: threading.Lock,
    semaphore: threading.BoundedSemaphore,
    stop_event: threading.Event,
) -> dict[str, Any]:
    """Execute and persist one slot before making its capacity available."""
    try:
        record = _request_record(
            scheduled,
            request_id=request_id,
            phase_started_monotonic=phase_started_monotonic,
            phase_end_monotonic=phase_end_monotonic,
            config=config,
            workload=workload,
            transport=transport,
            monotonic=monotonic,
            utc_now=utc_now,
            stop_event=stop_event,
        )
        return _emit_request(ledger, records, records_lock, record)
    except BaseException:
        stop_event.set()
        raise
    finally:
        semaphore.release()


def run_open_loop(
    config: RunConfig,
    workload: Workload,
    ledger: EventLedger,
    *,
    transport: Transport,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    utc_now: Callable[[], datetime] = lambda: datetime.now(UTC),
    request_id_factory: Callable[[], str] = lambda: str(uuid.uuid4()),
    executor_factory: Callable[[int], Executor] = lambda workers: ThreadPoolExecutor(
        max_workers=workers,
        thread_name_prefix="hm-w5-load",
    ),
    stop_event: threading.Event | None = None,
    records: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], str, str | None]:
    """Run all phases with absolute deadlines and bounded in-flight work."""
    stop_event = stop_event or threading.Event()
    semaphore = threading.BoundedSemaphore(config.max_in_flight)
    records = records if records is not None else []
    records_lock = threading.Lock()
    executor = executor_factory(config.max_in_flight)
    next_request_ordinal = 1
    terminal_status = "completed"
    stop_reason: str | None = None

    try:
        for phase in config.phases():
            if stop_event.is_set():
                terminal_status = "interrupted"
                stop_reason = "stop_requested"
                break
            ledger.write(
                "phase_started",
                phase=phase.name,
                trial_index=phase.trial_index,
                duration_seconds=phase.duration_seconds,
                started_at_utc=_utc_z(utc_now()),
            )
            phase_started = monotonic()
            phase_end = phase_started + phase.duration_seconds
            scheduled_requests = schedule_phase(
                phase,
                config.target_rps,
                first_request_ordinal=next_request_ordinal,
            )
            next_request_ordinal += len(scheduled_requests)
            futures: list[Future] = []

            if phase.name == "cooldown":
                _sleep_until(
                    phase_started + phase.duration_seconds,
                    monotonic=monotonic,
                    sleep=sleep,
                    stop_event=stop_event,
                )
            else:
                for scheduled in scheduled_requests:
                    if stop_event.is_set():
                        terminal_status = "interrupted"
                        stop_reason = "stop_requested"
                        break
                    deadline = phase_started + scheduled.scheduled_offset_seconds
                    if not _sleep_until(
                        deadline,
                        monotonic=monotonic,
                        sleep=sleep,
                        stop_event=stop_event,
                    ):
                        terminal_status = "interrupted"
                        stop_reason = "stop_requested"
                        break
                    if stop_event.is_set():
                        terminal_status = "interrupted"
                        stop_reason = "stop_requested"
                        break
                    now = monotonic()
                    schedule_lag_ms = max(0.0, (now - deadline) * 1000.0)
                    request_id = request_id_factory()
                    if schedule_lag_ms > config.max_schedule_lag_ms:
                        _emit_request(
                            ledger,
                            records,
                            records_lock,
                            _dropped_request_record(
                                scheduled,
                                request_id=request_id,
                                outcome="schedule_lag_drop",
                                schedule_lag_ms=schedule_lag_ms,
                                workload=workload,
                            ),
                        )
                        continue
                    if not semaphore.acquire(blocking=False):
                        _emit_request(
                            ledger,
                            records,
                            records_lock,
                            _dropped_request_record(
                                scheduled,
                                request_id=request_id,
                                outcome="client_overload_drop",
                                schedule_lag_ms=schedule_lag_ms,
                                workload=workload,
                            ),
                        )
                        continue

                    if stop_event.is_set():
                        semaphore.release()
                        terminal_status = "interrupted"
                        stop_reason = "stop_requested"
                        break

                    try:
                        future = executor.submit(
                            _execute_request_and_emit,
                            scheduled,
                            request_id=request_id,
                            phase_started_monotonic=phase_started,
                            phase_end_monotonic=phase_end,
                            config=config,
                            workload=workload,
                            transport=transport,
                            monotonic=monotonic,
                            utc_now=utc_now,
                            ledger=ledger,
                            records=records,
                            records_lock=records_lock,
                            semaphore=semaphore,
                            stop_event=stop_event,
                        )
                    except BaseException:
                        semaphore.release()
                        raise

                    futures.append(future)

                if not stop_event.is_set():
                    _sleep_until(
                        phase_started + phase.duration_seconds,
                        monotonic=monotonic,
                        sleep=sleep,
                        stop_event=stop_event,
                    )
                done, pending = wait(futures, timeout=config.drain_timeout_seconds)
                del done
                if pending:
                    stop_event.set()
                    terminal_status = "failed"
                    stop_reason = "drain_timeout"
                    ledger.write(
                        "drain_timeout_detected",
                        phase=phase.name,
                        trial_index=phase.trial_index,
                        detected_at_utc=_utc_z(utc_now()),
                        threshold_seconds=config.drain_timeout_seconds,
                        pending_futures=len(pending),
                    )
                    for future in pending:
                        if future.cancel():
                            semaphore.release()
                    # Threads already inside the transport cannot be killed safely.
                    # The detection threshold records failure immediately; sealing
                    # then waits for the separately bounded HTTP request timeout.
                    wait(futures)
                future_errors = [
                    future.exception()
                    for future in futures
                    if not future.cancelled() and future.exception() is not None
                ]
                if future_errors:
                    terminal_status = "failed"
                    stop_reason = "event_write_failure"
                unsafe_responses = [
                    row
                    for row in records
                    if row.get("phase") == phase.name
                    and row.get("trial_index") == phase.trial_index
                    and row.get("outcome") == "unsafe_stateful_response"
                ]
                if unsafe_responses:
                    terminal_status = "failed"
                    stop_reason = "unsafe_stateful_response"

            phase_records = [
                row
                for row in records
                if row.get("phase") == phase.name and row.get("trial_index") == phase.trial_index
            ]
            ledger.write(
                "phase_ended",
                phase=phase.name,
                trial_index=phase.trial_index,
                ended_at_utc=_utc_z(utc_now()),
                elapsed_seconds=max(0.0, monotonic() - phase_started),
                scheduled_requests=len(scheduled_requests),
                request_events=len(phase_records),
            )
            if terminal_status != "completed":
                break
    finally:
        executor.shutdown(wait=True, cancel_futures=True)

    return records, terminal_status, stop_reason


def _percentile(values: Iterable[float], q: float) -> float | None:
    """R-7 linear interpolation, matching bench_score.py's actual arithmetic."""
    ordered = sorted(values)
    if not ordered:
        return None
    if q <= 0:
        return ordered[0]
    if q >= 1:
        return ordered[-1]
    rank = q * (len(ordered) - 1)
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    fraction = rank - low
    return ordered[low] * (1 - fraction) + ordered[high] * fraction


def _distribution(values: Iterable[float]) -> dict[str, Any]:
    observed = list(values)
    return {
        "sample_count": len(observed),
        "mean": statistics.fmean(observed) if observed else None,
        "p50": _percentile(observed, 0.50),
        "p90": _percentile(observed, 0.90),
        "p95": _percentile(observed, 0.95),
        "p99": _percentile(observed, 0.99),
        "max": max(observed) if observed else None,
        "percentile_method": PERCENTILE_METHOD,
    }


def summarize_population(records: list[dict[str, Any]], duration_seconds: float) -> dict[str, Any]:
    scheduled = len(records)
    started = [row for row in records if row.get("started_at_utc") is not None]
    valid = [row for row in records if row.get("outcome") == "success"]
    good = [row for row in records if row.get("good") is True]
    outcomes = Counter(str(row.get("outcome")) for row in records)
    statuses = Counter(
        str(row["http_status"]) for row in records if row.get("http_status") is not None
    )
    return {
        "scheduled": scheduled,
        "started": len(started),
        "completed_attempts": len(started),
        "valid_http_200": len(valid),
        "good": len(good),
        "outcome_counts": dict(sorted(outcomes.items())),
        "http_status_counts": dict(sorted(statuses.items())),
        "success_ratio": len(valid) / scheduled if scheduled else None,
        "goodput_ratio": len(good) / scheduled if scheduled else None,
        "offered_rps": scheduled / duration_seconds if duration_seconds > 0 else None,
        "started_rps": len(started) / duration_seconds if duration_seconds > 0 else None,
        "valid_success_rps": len(valid) / duration_seconds if duration_seconds > 0 else None,
        "goodput_rps": len(good) / duration_seconds if duration_seconds > 0 else None,
        "client_latency_ms_all_attempts": _distribution(
            float(row["client_latency_ms"])
            for row in started
            if row.get("client_latency_ms") is not None
        ),
        "client_latency_ms_valid_success": _distribution(
            float(row["client_latency_ms"])
            for row in valid
            if row.get("client_latency_ms") is not None
        ),
        "scheduled_to_completion_ms_valid_success": _distribution(
            float(row["scheduled_to_completion_ms"])
            for row in valid
            if row.get("scheduled_to_completion_ms") is not None
        ),
        "server_latency_ms_valid_success": _distribution(
            float(row["server_latency_ms"])
            for row in valid
            if row.get("server_latency_ms") is not None
        ),
        "schedule_lag_ms_started": _distribution(
            float(row["schedule_lag_ms"])
            for row in started
            if row.get("schedule_lag_ms") is not None
        ),
    }


def _variation(values: list[float]) -> dict[str, float | int | None]:
    return {
        "sample_count": len(values),
        "mean": statistics.fmean(values) if values else None,
        "sample_standard_deviation": statistics.stdev(values) if len(values) >= 2 else None,
        "min": min(values) if values else None,
        "max": max(values) if values else None,
    }


def _soak_buckets(
    records: list[dict[str, Any]],
    duration_seconds: float,
    bucket_seconds: int = 60,
) -> list[dict]:
    buckets: dict[int, list[dict[str, Any]]] = {}
    for row in records:
        offset = float(row["scheduled_offset_seconds"])
        buckets.setdefault(int(offset // bucket_seconds), []).append(row)
    bucket_count = math.ceil(duration_seconds / bucket_seconds)
    return [
        {
            "bucket_index": index,
            "start_offset_seconds": index * bucket_seconds,
            "end_offset_seconds": min((index + 1) * bucket_seconds, duration_seconds),
            **summarize_population(
                buckets.get(index, []),
                min(bucket_seconds, duration_seconds - index * bucket_seconds),
            ),
        }
        for index in range(bucket_count)
    ]


def build_summary(
    config: RunConfig,
    records: list[dict[str, Any]],
    *,
    terminal_status: str,
    stop_reason: str | None,
    preflight: dict[str, Any],
    started_at: datetime,
    ended_at: datetime,
    elapsed_seconds: float,
    run_spec_sha256: str,
    events_sha256: str,
) -> dict[str, Any]:
    measured = [row for row in records if row.get("phase") == "trial"]
    warmup = [row for row in records if row.get("phase") == "warmup"]
    trial_summaries = []
    for trial_index in range(1, config.trials + 1):
        trial_records = [row for row in measured if row.get("trial_index") == trial_index]
        trial_summaries.append(
            {
                "trial_index": trial_index,
                **summarize_population(trial_records, config.trial_seconds),
            }
        )
    pooled = summarize_population(measured, config.trials * config.trial_seconds)
    preflight_trace_ids = [str(preflight["trace_id"])] if preflight.get("trace_id") else []
    warmup_trace_ids = [str(row["trace_id"]) for row in warmup if row.get("trace_id")]
    measured_trace_ids = [str(row["trace_id"]) for row in measured if row.get("trace_id")]
    trace_ids = preflight_trace_ids + warmup_trace_ids + measured_trace_ids
    duplicate_trace_ids = len(trace_ids) - len(set(trace_ids))
    request_ids = [str(row["request_id"]) for row in records if row.get("request_id")]
    duplicate_request_ids = len(request_ids) - len(set(request_ids))
    unsafe_response_count = sum(row.get("outcome") == "unsafe_stateful_response" for row in records)
    started_over_lag_limit = sum(
        row.get("started_at_utc") is not None
        and float(row.get("schedule_lag_ms", 0.0)) > config.max_schedule_lag_ms
        for row in records
    )
    request_ordinals = [int(row["request_ordinal"]) for row in records]
    ordinals_are_exact = sorted(request_ordinals) == list(
        range(1, config.planned_request_count + 1)
    )
    accounted = len(records) == config.planned_request_count
    per_trial_accounted = all(
        summary["scheduled"] == config.measurement_requests_per_trial for summary in trial_summaries
    )
    valid_for_measurement = (
        terminal_status == "completed"
        and preflight.get("attempted") is True
        and preflight.get("client_outcome_known") is True
        and preflight.get("outcome") == "success"
        and accounted
        and per_trial_accounted
        and duplicate_trace_ids == 0
        and duplicate_request_ids == 0
        and ordinals_are_exact
        and unsafe_response_count == 0
        and started_over_lag_limit == 0
    )
    success_ratio = pooled["success_ratio"]
    goodput_ratio = pooled["goodput_ratio"]
    performance_gates = [
        {
            "name": "minimum_success_ratio",
            "observed": success_ratio,
            "comparator": ">=",
            "threshold": config.minimum_success_ratio,
            "passed": success_ratio is not None and success_ratio >= config.minimum_success_ratio,
        },
        {
            "name": "minimum_goodput_ratio",
            "observed": goodput_ratio,
            "comparator": ">=",
            "threshold": config.minimum_goodput_ratio,
            "passed": goodput_ratio is not None and goodput_ratio >= config.minimum_goodput_ratio,
        },
    ]
    targets_met = valid_for_measurement and all(gate["passed"] for gate in performance_gates)
    p95_values = [
        float(summary["client_latency_ms_valid_success"]["p95"])
        for summary in trial_summaries
        if summary["client_latency_ms_valid_success"]["p95"] is not None
    ]
    goodput_values = [
        float(summary["goodput_rps"])
        for summary in trial_summaries
        if summary["goodput_rps"] is not None
    ]
    preflight_attempts = int(preflight.get("attempted") is True)
    preflight_client_outcome_known = preflight.get("client_outcome_known") is True
    summary = {
        "schema_version": SCHEMA_VERSION,
        "run_id": config.run_id,
        "status": terminal_status,
        "stop_reason": stop_reason,
        "valid_for_measurement": valid_for_measurement,
        "targets_met": targets_met,
        "verdict": (
            "passed"
            if targets_met
            else "targets_not_met"
            if valid_for_measurement
            else "invalid_measurement"
        ),
        "measurement_scope": "client-observed signed API Gateway request plus server latency field",
        "started_at_utc": _utc_z(started_at),
        "ended_at_utc": _utc_z(ended_at),
        "elapsed_seconds": elapsed_seconds,
        "run_spec_sha256": run_spec_sha256,
        "events_sha256": events_sha256,
        "preflight": preflight,
        "accounting": {
            "max_total_requests": config.max_total_requests,
            "planned_client_requests_including_preflight": (config.planned_total_client_requests),
            "preflight_attempts": preflight_attempts,
            "preflight_client_outcome_known": preflight_client_outcome_known,
            "actual_client_attempts_including_preflight": (
                preflight_attempts + sum(row.get("started_at_utc") is not None for row in records)
            ),
            "planned_scheduled_requests": config.planned_request_count,
            "request_events": len(records),
            "warmup_request_events": len(warmup),
            "measured_request_events": len(measured),
            "all_planned_requests_accounted": accounted,
            "each_trial_fully_accounted": per_trial_accounted,
            "unsafe_response_events": unsafe_response_count,
            "started_over_schedule_lag_limit": started_over_lag_limit,
        },
        "trace_identity": {
            "scope": "all_observed_trace_ids_across_preflight_warmup_and_measured_trials",
            "preflight_observed_trace_ids": len(preflight_trace_ids),
            "warmup_observed_trace_ids": len(warmup_trace_ids),
            "measured_observed_trace_ids": len(measured_trace_ids),
            "observed_trace_ids": len(trace_ids),
            "unique_trace_ids": len(set(trace_ids)),
            "duplicate_trace_ids": duplicate_trace_ids,
            "all_observed_trace_ids_unique": duplicate_trace_ids == 0,
        },
        "client_request_identity": {
            "request_ids": len(request_ids),
            "unique_request_ids": len(set(request_ids)),
            "duplicate_request_ids": duplicate_request_ids,
            "ordinals_are_exact": ordinals_are_exact,
        },
        "performance_gates": performance_gates,
        "trial_summaries": trial_summaries,
        "pooled_measured_trials": pooled,
        "trial_variation": {
            "goodput_rps": _variation(goodput_values),
            "client_success_p95_ms": _variation(p95_values),
        },
        "billing": {
            "status": "not_collected",
            "total_cost_usd": None,
            "cost_per_valid_inference_usd": None,
        },
    }
    if config.kind == "soak":
        summary["soak_60_second_buckets"] = _soak_buckets(
            measured,
            config.trial_seconds,
        )
    return summary


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_runtime_environment() -> None:
    """Reject ambient network overrides that change the reviewed paths."""
    blocked = (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "AWS_CA_BUNDLE",
        "REQUESTS_CA_BUNDLE",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
    )
    present = {name for name in blocked if os.environ.get(name)}
    present.update(
        name
        for name, value in os.environ.items()
        if value and (name == "AWS_ENDPOINT_URL" or name.startswith("AWS_ENDPOINT_URL_"))
    )
    if present:
        raise ValueError(
            f"unsupported proxy, CA, or endpoint override variables are set: {sorted(present)}"
        )


def validate_reviewed_source(config: RunConfig) -> dict[str, Any]:
    """Bind a run to one clean reviewed commit and exact imported source."""
    head = subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        text=True,
        stderr=subprocess.DEVNULL,
    ).strip()
    status = subprocess.check_output(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"],
        cwd=REPO_ROOT,
        text=True,
        stderr=subprocess.DEVNULL,
    ).splitlines()
    if head != config.expected_git_head:
        raise ValueError(f"reviewed source HEAD mismatch: {head}")
    if status:
        raise ValueError("reviewed source worktree is not clean")

    paths = {
        "harness": Path(__file__).resolve(),
        "window_logic": REPO_ROOT / "streaming" / "flink" / "window_logic.py",
        "models": REPO_ROOT / "serving" / "app" / "models.py",
    }
    actual = {name: _sha256(path) for name, path in paths.items()}
    expected = {
        "harness": config.expected_harness_sha256,
        "window_logic": config.expected_window_logic_sha256,
        "models": config.expected_models_sha256,
    }
    mismatches = sorted(name for name in actual if actual[name] != expected[name])
    if mismatches:
        raise ValueError(f"reviewed source checksum mismatch: {mismatches}")
    return {
        "git_head": head,
        "worktree_clean": True,
        "sha256": actual,
    }


def _expected_target_binding(
    config: RunConfig | SupervisorGrant,
) -> dict[str, str]:
    """Return the exact route and private integration reviewed for this run."""
    return {
        "api_id": config.expected_api_id,
        "score_path": EXPECTED_SCORE_PATH,
        "route_key": EXPECTED_ROUTE_KEY,
        "route_authorization": EXPECTED_ROUTE_AUTHORIZATION,
        "route_target": f"integrations/{config.expected_integration_id}",
        "integration_id": config.expected_integration_id,
        "integration_uri": config.expected_integration_uri,
        "integration_type": EXPECTED_INTEGRATION_TYPE,
        "integration_method": EXPECTED_INTEGRATION_METHOD,
        "connection_type": EXPECTED_CONNECTION_TYPE,
    }


def validate_live_ownership(config: RunConfig) -> dict[str, Any]:
    """Read-only AWS identity and API ownership checks before any scoring POST."""
    import botocore.session
    from botocore.config import Config

    session = botocore.session.get_session()
    client_config = Config(
        region_name=EXPECTED_REGION,
        connect_timeout=5.0,
        read_timeout=5.0,
        retries={"total_max_attempts": 1, "mode": "standard"},
        proxies={},
        use_fips_endpoint=False,
        use_dualstack_endpoint=False,
        ignore_configured_endpoint_urls=True,
    )
    sts = session.create_client(
        "sts",
        region_name=EXPECTED_REGION,
        endpoint_url=OFFICIAL_STS_ENDPOINT,
        config=client_config,
        verify=True,
    )
    apigw = session.create_client(
        "apigatewayv2",
        region_name=EXPECTED_REGION,
        endpoint_url=OFFICIAL_APIGATEWAYV2_ENDPOINT,
        config=client_config,
        verify=True,
    )
    if sts.meta.endpoint_url.rstrip("/") != OFFICIAL_STS_ENDPOINT:
        raise ValueError("STS control-plane endpoint mismatch")
    if apigw.meta.endpoint_url.rstrip("/") != OFFICIAL_APIGATEWAYV2_ENDPOINT:
        raise ValueError("API Gateway control-plane endpoint mismatch")
    caller = sts.get_caller_identity()
    api = apigw.get_api(ApiId=config.expected_api_id)
    routes = _read_api_routes(apigw, config.expected_api_id)
    integration = apigw.get_integration(
        ApiId=config.expected_api_id,
        IntegrationId=config.expected_integration_id,
    )
    attestation = _validate_ownership_records(config, caller, api, routes, integration)
    attestation.update(
        {
            "region": EXPECTED_REGION,
            "control_plane_endpoints": {
                "sts": OFFICIAL_STS_ENDPOINT,
                "apigatewayv2": OFFICIAL_APIGATEWAYV2_ENDPOINT,
            },
            "configured_endpoint_urls_ignored": True,
            "proxies_disabled": True,
        }
    )
    return attestation


def _read_api_routes(client: Any, api_id: str) -> list[dict[str, Any]]:
    """Read a bounded, non-repeating set of API Gateway route pages."""
    routes: list[dict[str, Any]] = []
    next_token: str | None = None
    seen_tokens: set[str] = set()
    for _page in range(MAX_ROUTE_PAGES):
        kwargs: dict[str, Any] = {"ApiId": api_id, "MaxResults": "100"}
        if next_token is not None:
            kwargs["NextToken"] = next_token
        response = client.get_routes(**kwargs)
        items = response.get("Items")
        if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
            raise ValueError("API Gateway routes response is malformed")
        routes.extend(items)
        raw_next = response.get("NextToken")
        if raw_next is None:
            return routes
        if not isinstance(raw_next, str) or not raw_next or raw_next in seen_tokens:
            raise ValueError("API Gateway routes pagination token is invalid")
        seen_tokens.add(raw_next)
        next_token = raw_next
    raise ValueError("API Gateway routes exceeded the bounded page limit")


def _validate_ownership_records(
    config: RunConfig,
    caller: dict[str, Any],
    api: dict[str, Any],
    routes: list[dict[str, Any]],
    integration: dict[str, Any],
) -> dict[str, Any]:
    """Validate sanitized read-only STS and API Gateway responses."""
    account = str(caller.get("Account", ""))
    caller_arn = str(caller.get("Arn", ""))
    api_endpoint = str(api.get("ApiEndpoint", "")).rstrip("/")
    expected_endpoint = (
        f"https://{config.expected_api_id}.execute-api.{config.region}.amazonaws.com"
    )
    tags = api.get("Tags") if isinstance(api.get("Tags"), dict) else {}
    if account != EXPECTED_ACCOUNT_ID:
        raise ValueError(f"unexpected AWS account: {account}")
    if not PLATFORM_CALLER_RE.fullmatch(caller_arn):
        raise ValueError(f"unexpected AWS caller ARN: {caller_arn}")
    if api.get("Name") != EXPECTED_API_NAME:
        raise ValueError(f"unexpected API Gateway name: {api.get('Name')}")
    if api.get("ProtocolType") != "HTTP" or api_endpoint != expected_endpoint:
        raise ValueError("API Gateway endpoint ownership mismatch")
    expected_tags = {
        "Project": "harbormaster",
        "Environment": "base",
        "ManagedBy": "terraform",
    }
    if any(tags.get(key) != value for key, value in expected_tags.items()):
        raise ValueError("API Gateway ownership tags mismatch")
    matching_routes = [route for route in routes if route.get("RouteKey") == EXPECTED_ROUTE_KEY]
    if len(matching_routes) != 1:
        raise ValueError("API Gateway must have exactly one reviewed scoring route")
    route = matching_routes[0]
    if route.get("AuthorizationType") != EXPECTED_ROUTE_AUTHORIZATION:
        raise ValueError("API Gateway scoring route must require AWS_IAM")
    expected_binding = _expected_target_binding(config)
    if route.get("Target") != expected_binding["route_target"]:
        raise ValueError("API Gateway scoring route target mismatch")
    if not isinstance(integration, dict):
        raise ValueError("API Gateway integration response is malformed")
    actual_integration = {
        "integration_id": integration.get("IntegrationId"),
        "integration_uri": integration.get("IntegrationUri"),
        "integration_type": integration.get("IntegrationType"),
        "integration_method": integration.get("IntegrationMethod"),
        "connection_type": integration.get("ConnectionType"),
    }
    expected_integration = {
        key: expected_binding[key]
        for key in (
            "integration_id",
            "integration_uri",
            "integration_type",
            "integration_method",
            "connection_type",
        )
    }
    if actual_integration != expected_integration:
        raise ValueError("API Gateway private integration does not match the reviewed target")
    return {
        "account": account,
        "caller_arn": caller_arn,
        "api_id": config.expected_api_id,
        "api_name": api["Name"],
        "api_endpoint": api_endpoint,
        "api_tags": expected_tags,
        "route": {
            "route_id": route.get("RouteId"),
            "route_key": route["RouteKey"],
            "authorization_type": route["AuthorizationType"],
            "target": route.get("Target"),
        },
        "integration": actual_integration,
    }


def _write_bytes_exclusive(path: Path, data: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def _write_json_exclusive(path: Path, payload: dict[str, Any]) -> None:
    data = json.dumps(payload, allow_nan=False, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    _write_bytes_exclusive(path, data)


def _git_metadata() -> dict[str, Any]:
    try:
        head = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        status = subprocess.check_output(
            ["git", "status", "--porcelain=v1", "--untracked-files=all"],
            cwd=REPO_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).splitlines()
    except (OSError, subprocess.CalledProcessError):
        return {"head": None, "dirty": None}
    return {"head": head, "dirty": bool(status)}


def _run_spec(
    config: RunConfig,
    workload: Workload,
    started_at: datetime,
    *,
    source_attestation: dict[str, Any],
    ownership_attestation: dict[str, Any],
    supervisor_grant: SupervisorGrant | None,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": config.run_id,
        "kind": config.kind,
        "arrival_model": "open_loop_absolute_deadlines_no_catch_up",
        "retry_policy": "none",
        "transport_connection_reuse": False,
        "started_at_utc": _utc_z(started_at),
        "target": {
            "api_url": config.api_url,
            "region": config.region,
            "reviewed_binding": _expected_target_binding(config),
            "ownership": ownership_attestation,
        },
        "reviewed_source": source_attestation,
        "evidence_binding": (
            {
                "mode": "supervised_live_cli",
                "claim_id": supervisor_grant.claim_id,
                "claim_filename": supervisor_grant.claim_filename,
                "claim_sha256": supervisor_grant.claim_sha256,
                "run_id": supervisor_grant.run_id,
                "artifact_dir": supervisor_grant.artifact_dir,
            }
            if supervisor_grant is not None
            else {"mode": "offline_injected_transport"}
        ),
        "config": {
            key: str(value) if isinstance(value, Path) else value
            for key, value in asdict(config).items()
            if key not in {"api_url", "region", "run_id", "artifact_dir", "kind"}
        },
        "planned_scheduled_request_count": config.planned_request_count,
        "planned_client_requests_including_preflight": (config.planned_total_client_requests),
        "measurement_start_condition": "successful scoring preflight, therefore prewarmed",
        "cold_start_claim_supported": False,
        "workload": {
            "identifier": workload.identifier,
            "sha256": workload.sha256,
            "bytes": len(workload.body),
            "mmsi": workload.mmsi,
            "claim": "one fixed boring AIS request, not a representative traffic mix",
        },
        "software": {
            "git": _git_metadata(),
            "python": platform.python_version(),
            "implementation": platform.python_implementation(),
            "platform": platform.platform(),
            "machine": platform.machine(),
        },
        "evidence_contract": {
            "client_and_server_latency_are_separate": True,
            "warmup_excluded_from_measured_trials": True,
            "preflight_is_billable_and_excluded_from_measured_trials": True,
            "cold_start_claim_supported": False,
            "drain_timeout_is_detection_not_process_kill": True,
            "cost_requires_separate_billing_artifact": True,
            "live_behavior_claimed_by_this_authored_tool": False,
        },
    }


def _preflight_record(
    config: RunConfig,
    workload: Workload,
    *,
    transport: Transport,
    monotonic: Callable[[], float],
) -> dict[str, Any]:
    started = monotonic()
    try:
        raw = transport(
            config.api_url,
            config.region,
            workload.body,
            config.request_timeout_seconds,
        )
    except Exception:
        return {
            "outcome": "transport_error",
            "error_code": "transport_exception",
            "http_status": None,
            "client_latency_ms": max(0.0, (monotonic() - started) * 1000.0),
        }
    latency_ms = max(0.0, (monotonic() - started) * 1000.0)
    if raw.error_code is not None:
        return {
            "outcome": "transport_error",
            "error_code": raw.error_code,
            "http_status": raw.status_code,
            "client_latency_ms": latency_ms,
        }
    if raw.status_code != 200:
        return {
            "outcome": "http_error",
            "error_code": f"http_{raw.status_code}",
            "http_status": raw.status_code,
            "client_latency_ms": latency_ms,
        }
    try:
        payload = validate_ais_score_response(raw.status_code, raw.body, workload.mmsi)
        observation = _response_observation(payload)
    except ValueError:
        return {
            "outcome": "contract_error",
            "error_code": "invalid_ais_score_response",
            "http_status": raw.status_code,
            "client_latency_ms": latency_ms,
        }
    if not _safe_boring_response(observation):
        return {
            "outcome": "unsafe_stateful_response",
            "error_code": "response_outside_boring_non_hitl_envelope",
            "http_status": 200,
            "client_latency_ms": latency_ms,
            **observation,
        }
    return {
        "outcome": "success",
        "error_code": None,
        "http_status": 200,
        "client_latency_ms": latency_ms,
        **observation,
    }


def _try_ledger_write(ledger: EventLedger, record_type: str, **fields: Any) -> bool:
    try:
        ledger.write(record_type, **fields)
    except BaseException:
        return False
    return True


def execute_run(
    config: RunConfig,
    workload: Workload,
    *,
    live_confirmation: str,
    transport: Transport,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    utc_now: Callable[[], datetime] = lambda: datetime.now(UTC),
    request_id_factory: Callable[[], str] = lambda: str(uuid.uuid4()),
    executor_factory: Callable[[int], Executor] = lambda workers: ThreadPoolExecutor(
        max_workers=workers,
        thread_name_prefix="hm-w5-load",
    ),
    stop_event: threading.Event | None = None,
    supervisor_grant: SupervisorGrant | None = None,
) -> tuple[dict[str, Any], int]:
    """Claim a fresh evidence directory, execute one bounded run, and seal it."""
    if live_confirmation != CONFIRM_PHRASE:
        raise ValueError(f"live_confirmation must equal: {CONFIRM_PHRASE}")
    if transport is signed_http_request and supervisor_grant is None:
        raise ValueError("live signed transport requires the inherited supervisor grant")
    if supervisor_grant is not None:
        if supervisor_grant is not _ACTIVE_SUPERVISOR_GRANT:
            raise ValueError("live execution requires the active inherited supervisor grant")
        _validate_supervisor_grant(config, supervisor_grant)
    validate_runtime_environment()
    source_attestation = validate_reviewed_source(config)
    ownership_attestation = validate_live_ownership(config)
    artifact_dir = config.artifact_dir.expanduser().resolve()
    if not artifact_dir.parent.is_dir():
        raise ValueError(f"artifact directory parent does not exist: {artifact_dir.parent}")
    os.mkdir(artifact_dir, 0o700)
    os.chmod(artifact_dir, 0o700)
    run_spec_path = artifact_dir / "run-spec.json"
    workload_path = artifact_dir / "workload.json"
    events_path = artifact_dir / "events.jsonl"
    summary_path = artifact_dir / "summary.json"
    manifest_path = artifact_dir / "evidence-files.sha256"

    started_at = utc_now()
    started_monotonic = monotonic()
    _write_bytes_exclusive(workload_path, workload.body + b"\n")
    run_spec = _run_spec(
        config,
        workload,
        started_at,
        source_attestation=source_attestation,
        ownership_attestation=ownership_attestation,
        supervisor_grant=supervisor_grant,
    )
    _write_json_exclusive(run_spec_path, run_spec)
    ledger = EventLedger(events_path, config.run_id)
    records: list[dict[str, Any]] = []
    terminal_status = "failed"
    stop_reason: str | None = "internal_error"
    preflight: dict[str, Any] = {
        "attempted": False,
        "client_outcome_known": False,
        "outcome": "not_run",
        "error_code": None,
    }
    exit_code = 1
    ledger_failed = False

    try:
        ledger.write("run_started", started_at_utc=_utc_z(started_at))
        preflight_started_at = utc_now()
        preflight = {
            "attempted": True,
            "client_outcome_known": False,
            "outcome": "unknown",
            "error_code": "preflight_outcome_unknown",
            "http_status": None,
            "client_latency_ms": None,
            "started_at_utc": _utc_z(preflight_started_at),
        }
        ledger.write(
            "preflight_attempt_started",
            started_at_utc=preflight["started_at_utc"],
            expected_mmsi=workload.mmsi,
            workload_sha256=workload.sha256,
        )
        preflight_result = _preflight_record(
            config,
            workload,
            transport=transport,
            monotonic=monotonic,
        )
        preflight = {
            "attempted": True,
            "client_outcome_known": True,
            "started_at_utc": preflight["started_at_utc"],
            **preflight_result,
        }
        ledger.write("preflight_completed", **preflight)
        if preflight["outcome"] != "success":
            terminal_status = "failed"
            stop_reason = "preflight_failed"
        else:
            records, terminal_status, stop_reason = run_open_loop(
                config,
                workload,
                ledger,
                transport=transport,
                monotonic=monotonic,
                sleep=sleep,
                utc_now=utc_now,
                request_id_factory=request_id_factory,
                executor_factory=executor_factory,
                stop_event=stop_event,
                records=records,
            )
    except KeyboardInterrupt:
        terminal_status = "interrupted"
        stop_reason = "keyboard_interrupt"
        if stop_event is not None:
            stop_event.set()
        ledger_failed = not _try_ledger_write(
            ledger,
            "stop_triggered",
            reason=stop_reason,
        )
        exit_code = 130
    except BaseException:
        terminal_status = "failed"
        stop_reason = "internal_error"
        ledger_failed = not _try_ledger_write(
            ledger,
            "stop_triggered",
            reason=stop_reason,
        )
        exit_code = 1
    finally:
        ended_at = utc_now()
        elapsed_seconds = max(0.0, monotonic() - started_monotonic)
        ledger_failed = (
            not _try_ledger_write(
                ledger,
                "run_ended",
                status=terminal_status,
                stop_reason=stop_reason,
                ended_at_utc=_utc_z(ended_at),
                elapsed_seconds=elapsed_seconds,
            )
            or ledger_failed
        )
        try:
            ledger.close()
        except BaseException:
            ledger_failed = True

    failure_path: Path | None = None
    ledger_failed = ledger_failed or stop_reason == "event_write_failure"
    if ledger_failed:
        terminal_status = "failed"
        stop_reason = "event_ledger_failure"
        exit_code = 1
        failure_path = artifact_dir / "failure.json"
        _write_json_exclusive(
            failure_path,
            {
                "schema_version": SCHEMA_VERSION,
                "run_id": config.run_id,
                "status": terminal_status,
                "stop_reason": stop_reason,
                "valid_for_measurement": False,
            },
        )

    summary = build_summary(
        config,
        records,
        terminal_status=terminal_status,
        stop_reason=stop_reason,
        preflight=preflight,
        started_at=started_at,
        ended_at=ended_at,
        elapsed_seconds=elapsed_seconds,
        run_spec_sha256=_sha256(run_spec_path),
        events_sha256=_sha256(events_path),
    )
    _write_json_exclusive(summary_path, summary)
    manifest_members = [run_spec_path, workload_path, events_path, summary_path]
    if failure_path is not None:
        manifest_members.append(failure_path)
    manifest_lines = [f"{_sha256(path)}  {path.name}" for path in sorted(manifest_members)]
    _write_bytes_exclusive(manifest_path, ("\n".join(manifest_lines) + "\n").encode())

    if exit_code != 130:
        exit_code = 0 if summary["targets_met"] else 1
    return summary, exit_code


def _positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a number") from error
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be finite and > 0")
    return parsed


def _nonnegative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a number") from error
    if not math.isfinite(parsed) or parsed < 0:
        raise argparse.ArgumentTypeError("must be finite and >= 0")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", required=True, choices=("load", "soak"))
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--expected-api-id", required=True)
    parser.add_argument("--expected-integration-id", required=True)
    parser.add_argument("--expected-integration-uri", required=True)
    parser.add_argument("--expected-git-head", required=True)
    parser.add_argument("--expected-harness-sha256", required=True)
    parser.add_argument("--expected-window-logic-sha256", required=True)
    parser.add_argument("--expected-models-sha256", required=True)
    parser.add_argument("--operator-cutoff-utc", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--artifact-dir", required=True, type=Path)
    parser.add_argument("--workload", required=True, type=Path)
    parser.add_argument("--target-rps", required=True, type=_positive_float)
    parser.add_argument("--max-in-flight", required=True, type=int)
    parser.add_argument("--warmup-seconds", required=True, type=_nonnegative_float)
    parser.add_argument("--trial-seconds", required=True, type=_positive_float)
    parser.add_argument("--trials", required=True, type=int)
    parser.add_argument("--cooldown-seconds", required=True, type=_nonnegative_float)
    parser.add_argument("--request-timeout-seconds", required=True, type=_positive_float)
    parser.add_argument("--drain-timeout-seconds", required=True, type=_positive_float)
    parser.add_argument(
        "--goodput-max-scheduled-response-ms",
        required=True,
        type=_positive_float,
    )
    parser.add_argument("--max-schedule-lag-ms", required=True, type=_positive_float)
    parser.add_argument("--max-total-requests", required=True, type=int)
    parser.add_argument("--minimum-success-ratio", required=True, type=float)
    parser.add_argument("--minimum-goodput-ratio", required=True, type=float)
    parser.add_argument("--confirm-live", required=True)
    return parser


def config_from_args(args: argparse.Namespace) -> RunConfig:
    if args.confirm_live != CONFIRM_PHRASE:
        raise ValueError(f"confirm-live must equal: {CONFIRM_PHRASE}")
    return RunConfig(
        kind=args.kind,
        run_id=args.run_id,
        api_url=args.api_url,
        region=args.region,
        expected_api_id=args.expected_api_id,
        expected_integration_id=args.expected_integration_id,
        expected_integration_uri=args.expected_integration_uri,
        expected_git_head=args.expected_git_head,
        expected_harness_sha256=args.expected_harness_sha256,
        expected_window_logic_sha256=args.expected_window_logic_sha256,
        expected_models_sha256=args.expected_models_sha256,
        operator_cutoff_utc=args.operator_cutoff_utc,
        artifact_dir=args.artifact_dir,
        target_rps=args.target_rps,
        max_in_flight=args.max_in_flight,
        warmup_seconds=args.warmup_seconds,
        trial_seconds=args.trial_seconds,
        trials=args.trials,
        cooldown_seconds=args.cooldown_seconds,
        request_timeout_seconds=args.request_timeout_seconds,
        drain_timeout_seconds=args.drain_timeout_seconds,
        goodput_max_scheduled_response_ms=args.goodput_max_scheduled_response_ms,
        max_schedule_lag_ms=args.max_schedule_lag_ms,
        max_total_requests=args.max_total_requests,
        minimum_success_ratio=args.minimum_success_ratio,
        minimum_goodput_ratio=args.minimum_goodput_ratio,
    )


def _hash_artifact_files(artifact_dir: Path) -> dict[str, str]:
    if not artifact_dir.is_dir():
        return {}
    return {
        str(path.relative_to(artifact_dir)): _sha256(path)
        for path in sorted(artifact_dir.rglob("*"))
        if path.is_file()
    }


def _supervisor_paths(artifact_dir: Path) -> dict[str, Path]:
    base = artifact_dir.with_name(artifact_dir.name)
    claim = base.with_name(f"{base.name}.supervisor-claim.json")
    stop = base.with_name(f"{base.name}.supervisor-stop.json")
    complete = base.with_name(f"{base.name}.supervisor-complete.json")
    return {
        "claim": claim,
        "claim_manifest": claim.with_suffix(".sha256"),
        "stop": stop,
        "stop_manifest": stop.with_suffix(".sha256"),
        "complete": complete,
        "complete_manifest": complete.with_suffix(".sha256"),
    }


def _claim_supervisor_evidence(
    config: RunConfig,
    *,
    utc_now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> SupervisorClaim:
    """Exclusively consume the supervisor evidence namespace for one run."""
    artifact_dir = config.artifact_dir.expanduser().resolve()
    if not artifact_dir.parent.is_dir():
        raise ValueError(f"artifact directory parent does not exist: {artifact_dir.parent}")
    paths = _supervisor_paths(artifact_dir)
    collisions = [artifact_dir, *paths.values()]
    existing = [str(path) for path in collisions if path.exists()]
    if existing:
        raise FileExistsError(f"supervisor evidence path already exists: {existing}")

    claim_payload = {
        "schema_version": SCHEMA_VERSION,
        "record_type": "supervisor_claim",
        "claim_id": str(uuid.uuid4()),
        "run_id": config.run_id,
        "artifact_dir": str(artifact_dir),
        "operator_cutoff_utc": config.operator_cutoff_utc,
        "expected_api_id": config.expected_api_id,
        "target_binding": _expected_target_binding(config),
        "expected_git_head": config.expected_git_head,
        "reviewed_source_sha256": {
            "harness": config.expected_harness_sha256,
            "window_logic": config.expected_window_logic_sha256,
            "models": config.expected_models_sha256,
        },
        "claimed_at_utc": _utc_z(utc_now()),
    }
    _write_json_exclusive(paths["claim"], claim_payload)
    claim_sha256 = _sha256(paths["claim"])
    _write_bytes_exclusive(
        paths["claim_manifest"],
        f"{claim_sha256}  {paths['claim'].name}\n".encode(),
    )
    appeared = [
        str(path)
        for path in (
            artifact_dir,
            paths["stop"],
            paths["stop_manifest"],
            paths["complete"],
            paths["complete_manifest"],
        )
        if path.exists()
    ]
    if appeared:
        raise FileExistsError(f"supervisor evidence collision after claim: {appeared}")
    return SupervisorClaim(
        claim_id=claim_payload["claim_id"],
        claim_path=paths["claim"],
        claim_manifest_path=paths["claim_manifest"],
        claim_sha256=claim_sha256,
    )


def _build_supervisor_grant(
    config: RunConfig,
    claim: SupervisorClaim,
    *,
    nonce: str | None = None,
) -> SupervisorGrant:
    return SupervisorGrant(
        schema_version=SCHEMA_VERSION,
        nonce=nonce if nonce is not None else secrets.token_hex(32),
        parent_pid=os.getpid(),
        run_id=config.run_id,
        expected_api_id=config.expected_api_id,
        expected_integration_id=config.expected_integration_id,
        expected_integration_uri=config.expected_integration_uri,
        expected_git_head=config.expected_git_head,
        expected_harness_sha256=config.expected_harness_sha256,
        expected_window_logic_sha256=config.expected_window_logic_sha256,
        expected_models_sha256=config.expected_models_sha256,
        artifact_dir=str(config.artifact_dir.expanduser().resolve()),
        operator_cutoff_utc=config.operator_cutoff_utc,
        claim_id=claim.claim_id,
        claim_filename=claim.claim_path.name,
        claim_sha256=claim.claim_sha256,
    )


def _validate_supervisor_grant(config: RunConfig, grant: SupervisorGrant) -> None:
    expected = {
        "schema_version": SCHEMA_VERSION,
        "run_id": config.run_id,
        "expected_api_id": config.expected_api_id,
        "expected_integration_id": config.expected_integration_id,
        "expected_integration_uri": config.expected_integration_uri,
        "expected_git_head": config.expected_git_head,
        "expected_harness_sha256": config.expected_harness_sha256,
        "expected_window_logic_sha256": config.expected_window_logic_sha256,
        "expected_models_sha256": config.expected_models_sha256,
        "artifact_dir": str(config.artifact_dir.expanduser().resolve()),
        "operator_cutoff_utc": config.operator_cutoff_utc,
    }
    actual = {name: getattr(grant, name) for name in expected}
    if actual != expected:
        raise ValueError("supervisor grant does not match the reviewed run configuration")
    if grant.parent_pid != os.getppid():
        raise ValueError("supervisor grant parent process does not match")
    if not re.fullmatch(r"[0-9a-f]{64}", grant.nonce):
        raise ValueError("supervisor grant nonce is malformed")
    if not HEX_SHA256_RE.fullmatch(grant.claim_sha256):
        raise ValueError("supervisor grant claim digest is malformed")


def _consume_supervisor_grant(
    config: RunConfig,
    *,
    environment: MutableMapping[str, str] | None = None,
) -> tuple[SupervisorGrant, BinaryIO]:
    """Consume one bounded inherited pipe grant and verify its immutable claim."""
    global _ACTIVE_SUPERVISOR_GRANT

    environment = os.environ if environment is None else environment
    raw_fd = environment.pop("HM_W5_SUPERVISOR_FD", None)
    if raw_fd is None or not raw_fd.isdigit():
        raise ValueError("supervised child is missing its inherited capability descriptor")
    fd = int(raw_fd)
    try:
        reader = os.fdopen(fd, "rb", buffering=0)
    except OSError as error:
        raise ValueError("supervisor capability descriptor is not open") from error
    try:
        encoded = reader.readline(4097)
        if not encoded.endswith(b"\n") or len(encoded) > 4096:
            raise ValueError("supervisor capability payload is malformed")
        payload = json.loads(encoded)
        if not isinstance(payload, dict):
            raise ValueError("supervisor capability payload must be an object")
        grant = SupervisorGrant(**payload)
        _validate_supervisor_grant(config, grant)
        paths = _supervisor_paths(config.artifact_dir.expanduser().resolve())
        if paths["claim"].name != grant.claim_filename:
            raise ValueError("supervisor claim filename mismatch")
        if _sha256(paths["claim"]) != grant.claim_sha256:
            raise ValueError("supervisor claim checksum mismatch")
        expected_manifest = f"{grant.claim_sha256}  {grant.claim_filename}\n"
        if paths["claim_manifest"].read_text(encoding="utf-8") != expected_manifest:
            raise ValueError("supervisor claim manifest mismatch")
        claim_payload = json.loads(paths["claim"].read_text(encoding="utf-8"))
        if (
            claim_payload.get("claim_id") != grant.claim_id
            or claim_payload.get("run_id") != grant.run_id
            or claim_payload.get("artifact_dir") != grant.artifact_dir
            or claim_payload.get("operator_cutoff_utc") != grant.operator_cutoff_utc
            or claim_payload.get("expected_api_id") != grant.expected_api_id
            or claim_payload.get("target_binding") != _expected_target_binding(config)
            or claim_payload.get("expected_git_head") != grant.expected_git_head
            or claim_payload.get("reviewed_source_sha256")
            != {
                "harness": grant.expected_harness_sha256,
                "window_logic": grant.expected_window_logic_sha256,
                "models": grant.expected_models_sha256,
            }
        ):
            raise ValueError("supervisor claim binding mismatch")
    except BaseException:
        reader.close()
        raise
    _ACTIVE_SUPERVISOR_GRANT = grant
    return grant, reader


def _watch_parent_liveness(
    reader: BinaryIO,
    stop_event: threading.Event,
    worker_finished: threading.Event | None = None,
) -> None:
    """Turn parent pipe EOF into the same bounded child stop path as SIGTERM."""
    try:
        marker = reader.read(1)
    except OSError:
        marker = b""
    if marker == b"" and (worker_finished is None or not worker_finished.is_set()):
        stop_event.set()
        os.kill(os.getpid(), signal.SIGTERM)


def build_supervisor_plan(
    config: RunConfig,
    *,
    started_utc: datetime,
    started_monotonic: float,
) -> SupervisorPlan:
    cutoff = parse_utc_cutoff(config.operator_cutoff_utc)
    remaining = (cutoff - started_utc).total_seconds()
    required = config.required_supervised_window_seconds
    if remaining <= 0:
        raise ValueError("operator cutoff has already passed")
    if remaining > MAX_ACTIVE_SECONDS:
        raise ValueError("operator cutoff must be within four hours")
    if remaining < required:
        raise ValueError("operator cutoff does not fit the reviewed supervised window")
    hard_stop = started_monotonic + remaining
    return SupervisorPlan(
        cutoff_utc=cutoff,
        started_utc=started_utc,
        started_monotonic=started_monotonic,
        graceful_stop_monotonic=hard_stop - config.termination_grace_seconds,
        hard_stop_monotonic=hard_stop,
        required_window_seconds=required,
        termination_grace_seconds=config.termination_grace_seconds,
    )


def _write_supervisor_stop(
    artifact_dir: Path,
    *,
    grant: SupervisorGrant,
    reason: str,
    child_exit_code: int,
    terminate_sent: bool,
    kill_sent: bool,
    started_at: datetime,
    stopped_at: datetime,
) -> Path:
    paths = _supervisor_paths(artifact_dir)
    if paths["complete"].exists() or paths["complete_manifest"].exists():
        raise ValueError("supervisor complete verdict already exists")
    claim_path = paths["claim"]
    observed_claim_sha256 = _sha256(claim_path) if claim_path.is_file() else None
    payload = {
        "schema_version": SCHEMA_VERSION,
        "record_type": "supervisor_stop",
        "valid_for_measurement": False,
        "run_id": grant.run_id,
        "artifact_dir": grant.artifact_dir,
        "claim_id": grant.claim_id,
        "claim_filename": grant.claim_filename,
        "expected_claim_sha256": grant.claim_sha256,
        "observed_claim_sha256": observed_claim_sha256,
        "target_binding": _expected_target_binding(grant),
        "reason": reason,
        "child_exit_code": child_exit_code,
        "terminate_sent": terminate_sent,
        "kill_sent": kill_sent,
        "started_at_utc": _utc_z(started_at),
        "stopped_at_utc": _utc_z(stopped_at),
        "artifact_files_at_stop": _hash_artifact_files(artifact_dir),
    }
    _write_json_exclusive(paths["stop"], payload)
    _write_bytes_exclusive(
        paths["stop_manifest"],
        (
            f"{_sha256(paths['stop'])}  {paths['stop'].name}\n"
            f"{grant.claim_sha256}  {grant.claim_filename}\n"
        ).encode(),
    )
    return paths["stop"]


def _write_supervisor_complete(
    artifact_dir: Path,
    *,
    grant: SupervisorGrant,
    child_exit_code: int,
    started_at: datetime,
    completed_at: datetime,
) -> Path:
    """Write the positive parent verdict after evidence validation succeeds."""
    paths = _supervisor_paths(artifact_dir)
    if paths["stop"].exists() or paths["stop_manifest"].exists():
        raise ValueError("supervisor stop verdict already exists")
    summary_path = artifact_dir / "summary.json"
    evidence_manifest_path = artifact_dir / "evidence-files.sha256"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    payload = {
        "schema_version": SCHEMA_VERSION,
        "record_type": "supervisor_complete",
        "supervisor_accepted_evidence": True,
        "run_id": grant.run_id,
        "artifact_dir": grant.artifact_dir,
        "claim_id": grant.claim_id,
        "claim_filename": grant.claim_filename,
        "claim_sha256": grant.claim_sha256,
        "target_binding": _expected_target_binding(grant),
        "child_exit_code": child_exit_code,
        "started_at_utc": _utc_z(started_at),
        "completed_at_utc": _utc_z(completed_at),
        "summary": {
            "sha256": _sha256(summary_path),
            "status": summary.get("status"),
            "valid_for_measurement": summary.get("valid_for_measurement"),
            "targets_met": summary.get("targets_met"),
            "verdict": summary.get("verdict"),
        },
        "evidence_manifest_sha256": _sha256(evidence_manifest_path),
    }
    _write_json_exclusive(paths["complete"], payload)
    _write_bytes_exclusive(
        paths["complete_manifest"],
        (
            f"{_sha256(paths['complete'])}  {paths['complete'].name}\n"
            f"{grant.claim_sha256}  {grant.claim_filename}\n"
        ).encode(),
    )
    return paths["complete"]


def _verify_supervisor_complete(artifact_dir: Path, grant: SupervisorGrant) -> bool:
    """Require the one positive terminal parent verdict for acceptable evidence."""
    paths = _supervisor_paths(artifact_dir)
    try:
        if paths["stop"].exists() or paths["stop_manifest"].exists():
            return False
        if _sha256(paths["claim"]) != grant.claim_sha256:
            return False
        complete = json.loads(paths["complete"].read_text(encoding="utf-8"))
        if (
            complete.get("record_type") != "supervisor_complete"
            or complete.get("supervisor_accepted_evidence") is not True
            or complete.get("run_id") != grant.run_id
            or complete.get("artifact_dir") != grant.artifact_dir
            or complete.get("claim_id") != grant.claim_id
            or complete.get("claim_sha256") != grant.claim_sha256
            or complete.get("target_binding") != _expected_target_binding(grant)
        ):
            return False
        summary_path = artifact_dir / "summary.json"
        evidence_manifest_path = artifact_dir / "evidence-files.sha256"
        if complete.get("summary", {}).get("sha256") != _sha256(summary_path):
            return False
        if complete.get("evidence_manifest_sha256") != _sha256(evidence_manifest_path):
            return False
        expected_manifest = (
            f"{_sha256(paths['complete'])}  {paths['complete'].name}\n"
            f"{grant.claim_sha256}  {grant.claim_filename}\n"
        )
        return paths["complete_manifest"].read_text(encoding="utf-8") == expected_manifest
    except (FileNotFoundError, OSError, TypeError, ValueError, json.JSONDecodeError):
        return False


def _verify_evidence_binding(config: RunConfig, grant: SupervisorGrant) -> bool:
    artifact_dir = config.artifact_dir.expanduser().resolve()
    paths = _supervisor_paths(artifact_dir)
    try:
        if _sha256(paths["claim"]) != grant.claim_sha256:
            return False
        if paths["claim_manifest"].read_text(encoding="utf-8") != (
            f"{grant.claim_sha256}  {grant.claim_filename}\n"
        ):
            return False
        run_spec = json.loads((artifact_dir / "run-spec.json").read_text(encoding="utf-8"))
        binding = run_spec.get("evidence_binding")
        expected_binding = {
            "mode": "supervised_live_cli",
            "claim_id": grant.claim_id,
            "claim_filename": grant.claim_filename,
            "claim_sha256": grant.claim_sha256,
            "run_id": grant.run_id,
            "artifact_dir": grant.artifact_dir,
        }
        if binding != expected_binding or run_spec.get("run_id") != grant.run_id:
            return False
        target = run_spec.get("target")
        if not isinstance(target, dict):
            return False
        if target.get("api_url") != config.api_url or target.get("region") != config.region:
            return False
        if target.get("reviewed_binding") != _expected_target_binding(config):
            return False
        recorded_config = run_spec.get("config")
        if not isinstance(recorded_config, dict):
            return False
        if (
            recorded_config.get("expected_api_id") != config.expected_api_id
            or recorded_config.get("expected_integration_id") != config.expected_integration_id
            or recorded_config.get("expected_integration_uri") != config.expected_integration_uri
        ):
            return False
        ownership = target.get("ownership")
        if not isinstance(ownership, dict):
            return False
        expected_binding = _expected_target_binding(config)
        if (
            ownership.get("api_id") != expected_binding["api_id"]
            or ownership.get("route")
            != {
                "route_id": ownership.get("route", {}).get("route_id")
                if isinstance(ownership.get("route"), dict)
                else None,
                "route_key": expected_binding["route_key"],
                "authorization_type": expected_binding["route_authorization"],
                "target": expected_binding["route_target"],
            }
            or ownership.get("integration")
            != {
                key: expected_binding[key]
                for key in (
                    "integration_id",
                    "integration_uri",
                    "integration_type",
                    "integration_method",
                    "connection_type",
                )
            }
        ):
            return False
        manifest_path = artifact_dir / "evidence-files.sha256"
        lines = manifest_path.read_text(encoding="utf-8").splitlines()
        if not lines:
            return False
        seen: set[str] = set()
        for line in lines:
            digest, separator, filename = line.partition("  ")
            if (
                separator != "  "
                or not HEX_SHA256_RE.fullmatch(digest)
                or not filename
                or Path(filename).name != filename
                or filename in seen
            ):
                return False
            seen.add(filename)
            if _sha256(artifact_dir / filename) != digest:
                return False
        required = {"run-spec.json", "workload.json", "events.jsonl", "summary.json"}
        if not required.issubset(seen):
            return False
        summary = json.loads((artifact_dir / "summary.json").read_text(encoding="utf-8"))
        return isinstance(summary, dict) and summary.get("run_id") == grant.run_id
    except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
        return False


class _SupervisorInterrupted(BaseException):
    pass


def _supervisor_signals() -> tuple[int, ...]:
    return tuple(
        dict.fromkeys(
            (
                signal.SIGINT,
                signal.SIGTERM,
                getattr(signal, "SIGHUP", signal.SIGTERM),
            )
        )
    )


def _block_supervisor_signals() -> set[signal.Signals]:
    """Atomically block every signal that can interrupt the supervisor."""
    return signal.pthread_sigmask(signal.SIG_BLOCK, set(_supervisor_signals()))


@dataclass
class _SupervisorInterruptState:
    interrupted: bool = False

    def handle(self, _signum: int, _frame: Any) -> None:
        self.interrupted = True
        _block_supervisor_signals()


def _restore_signal_mask(mask: set[signal.Signals]) -> None:
    signal.pthread_sigmask(signal.SIG_SETMASK, mask)


def _unblock_supervisor_signals() -> None:
    signal.pthread_sigmask(signal.SIG_UNBLOCK, set(_supervisor_signals()))


def _pending_supervisor_signals() -> set[signal.Signals]:
    return set(signal.sigpending()).intersection(_supervisor_signals())


def _restore_supervisor_signal_state(
    previous_handlers: dict[int, Any],
    prior_signal_mask: set[signal.Signals],
) -> None:
    """Discard late interrupts before restoring the caller's signal policy."""
    _block_supervisor_signals()
    for signum in _supervisor_signals():
        signal.signal(signum, signal.SIG_IGN)
    _restore_signal_mask(prior_signal_mask)
    for signum, previous in previous_handlers.items():
        signal.signal(signum, previous)


def _close_supervisor_fd(fd: int) -> None:
    os.close(fd)


def _wait_for_child_until(
    child: subprocess.Popen,
    deadline: float,
    monotonic: Callable[[], float],
) -> bool:
    """Wait to one deadline while deferring repeated operator interrupts."""
    while True:
        try:
            child.wait(timeout=max(0.0, deadline - monotonic()))
            return True
        except (KeyboardInterrupt, _SupervisorInterrupted):
            continue
        except subprocess.TimeoutExpired:
            return False


def _stop_and_reap(
    child: subprocess.Popen,
    *,
    artifact_dir: Path,
    grant: SupervisorGrant,
    reason: str,
    started_at: datetime,
    cleanup_deadline: float,
    monotonic: Callable[[], float],
    utc_now: Callable[[], datetime],
) -> int:
    _block_supervisor_signals()
    terminate_sent = False
    kill_sent = False
    if child.poll() is None:
        child.terminate()
        terminate_sent = True
    if not _wait_for_child_until(child, cleanup_deadline, monotonic):
        child.kill()
        kill_sent = True
        reaped = _wait_for_child_until(
            child,
            monotonic() + POST_KILL_REAP_SECONDS,
            monotonic,
        )
        if not reaped:
            child.kill()
            while child.poll() is None:
                try:
                    child.wait()
                except (KeyboardInterrupt, _SupervisorInterrupted):
                    continue
    child_exit_code = int(child.returncode if child.returncode is not None else -1)
    _write_supervisor_stop(
        artifact_dir,
        grant=grant,
        reason="hard_deadline_kill" if reason == "operator_cutoff_stop" and kill_sent else reason,
        child_exit_code=child_exit_code,
        terminate_sent=terminate_sent,
        kill_sent=kill_sent,
        started_at=started_at,
        stopped_at=utc_now(),
    )
    return 124


def _supervise_child(
    command: list[str],
    *,
    environment: dict[str, str],
    artifact_dir: Path,
    grant: SupervisorGrant,
    plan: SupervisorPlan,
    evidence_validator: Callable[[], bool],
    monotonic: Callable[[], float] = time.monotonic,
    utc_now: Callable[[], datetime] = lambda: datetime.now(UTC),
    popen_factory: Callable[..., subprocess.Popen] = subprocess.Popen,
    prior_signal_mask: set[signal.Signals] | None = None,
) -> int:
    """Enforce an outer process deadline and preserve a separate stop verdict."""
    prior_signal_mask = (
        _block_supervisor_signals() if prior_signal_mask is None else prior_signal_mask
    )
    started_at = utc_now()
    read_fd = -1
    write_fd = -1
    child: subprocess.Popen | None = None
    interrupt_state = _SupervisorInterruptState()
    previous_handlers = {signum: signal.getsignal(signum) for signum in _supervisor_signals()}
    environment = environment.copy()
    environment.pop("HM_W5_SUPERVISOR_PARENT_PID", None)

    try:
        for signum in _supervisor_signals():
            signal.signal(signum, interrupt_state.handle)
        read_fd, write_fd = os.pipe()
        environment["HM_W5_SUPERVISOR_FD"] = str(read_fd)
        payload = json.dumps(asdict(grant), separators=(",", ":"), sort_keys=True).encode() + b"\n"
        if len(payload) > 4096:
            raise ValueError("supervisor capability payload exceeds 4096 bytes")
        if interrupt_state.interrupted or _pending_supervisor_signals():
            _write_supervisor_stop(
                artifact_dir,
                grant=grant,
                reason="child_start_interrupted",
                child_exit_code=-1,
                terminate_sent=False,
                kill_sent=False,
                started_at=started_at,
                stopped_at=utc_now(),
            )
            return 130
        child = popen_factory(
            command,
            env=environment,
            pass_fds=(read_fd,),
            start_new_session=True,
        )
        if interrupt_state.interrupted or _pending_supervisor_signals():
            _close_supervisor_fd(read_fd)
            read_fd = -1
            _close_supervisor_fd(write_fd)
            write_fd = -1
            _stop_and_reap(
                child,
                artifact_dir=artifact_dir,
                grant=grant,
                reason="supervisor_interrupted",
                started_at=started_at,
                cleanup_deadline=min(
                    plan.hard_stop_monotonic,
                    monotonic() + plan.termination_grace_seconds,
                ),
                monotonic=monotonic,
                utc_now=utc_now,
            )
            return 130
        _close_supervisor_fd(read_fd)
        read_fd = -1
        os.write(write_fd, payload)
        if interrupt_state.interrupted or _pending_supervisor_signals():
            _close_supervisor_fd(write_fd)
            write_fd = -1
            _stop_and_reap(
                child,
                artifact_dir=artifact_dir,
                grant=grant,
                reason="supervisor_interrupted",
                started_at=started_at,
                cleanup_deadline=min(
                    plan.hard_stop_monotonic,
                    monotonic() + plan.termination_grace_seconds,
                ),
                monotonic=monotonic,
                utc_now=utc_now,
            )
            return 130
        _restore_signal_mask(prior_signal_mask)
        while True:
            if interrupt_state.interrupted:
                _block_supervisor_signals()
                _close_supervisor_fd(write_fd)
                write_fd = -1
                _stop_and_reap(
                    child,
                    artifact_dir=artifact_dir,
                    grant=grant,
                    reason="supervisor_interrupted",
                    started_at=started_at,
                    cleanup_deadline=min(
                        plan.hard_stop_monotonic,
                        monotonic() + plan.termination_grace_seconds,
                    ),
                    monotonic=monotonic,
                    utc_now=utc_now,
                )
                return 130
            remaining = max(0.0, plan.graceful_stop_monotonic - monotonic())
            try:
                exit_code = int(
                    child.wait(timeout=min(SUPERVISOR_INTERRUPT_POLL_SECONDS, remaining))
                )
            except subprocess.TimeoutExpired:
                if interrupt_state.interrupted:
                    continue
                if monotonic() < plan.graceful_stop_monotonic:
                    continue
                return _stop_and_reap(
                    child,
                    artifact_dir=artifact_dir,
                    grant=grant,
                    reason="operator_cutoff_stop",
                    started_at=started_at,
                    cleanup_deadline=plan.hard_stop_monotonic,
                    monotonic=monotonic,
                    utc_now=utc_now,
                )
            _block_supervisor_signals()
            if interrupt_state.interrupted or _pending_supervisor_signals():
                _close_supervisor_fd(write_fd)
                write_fd = -1
                _stop_and_reap(
                    child,
                    artifact_dir=artifact_dir,
                    grant=grant,
                    reason="supervisor_interrupted",
                    started_at=started_at,
                    cleanup_deadline=min(
                        plan.hard_stop_monotonic,
                        monotonic() + plan.termination_grace_seconds,
                    ),
                    monotonic=monotonic,
                    utc_now=utc_now,
                )
                return 130
            break
        evidence_valid = evidence_validator()
        # This blocked check is the terminal-verdict linearization point. Signals
        # observed before it invalidate the run; later signals are post-verdict.
        if interrupt_state.interrupted or _pending_supervisor_signals():
            _close_supervisor_fd(write_fd)
            write_fd = -1
            _stop_and_reap(
                child,
                artifact_dir=artifact_dir,
                grant=grant,
                reason="supervisor_interrupted",
                started_at=started_at,
                cleanup_deadline=min(
                    plan.hard_stop_monotonic,
                    monotonic() + plan.termination_grace_seconds,
                ),
                monotonic=monotonic,
                utc_now=utc_now,
            )
            return 130
        if not evidence_valid:
            _block_supervisor_signals()
            _write_supervisor_stop(
                artifact_dir,
                grant=grant,
                reason="evidence_binding_failure",
                child_exit_code=exit_code,
                terminate_sent=False,
                kill_sent=False,
                started_at=started_at,
                stopped_at=utc_now(),
            )
            return 124
        _block_supervisor_signals()
        _write_supervisor_complete(
            artifact_dir,
            grant=grant,
            child_exit_code=exit_code,
            started_at=started_at,
            completed_at=utc_now(),
        )
        if not _verify_supervisor_complete(artifact_dir, grant):
            raise RuntimeError("supervisor complete verdict verification failed")
        return exit_code
    except (KeyboardInterrupt, _SupervisorInterrupted):
        _block_supervisor_signals()
        if child is None:
            _write_supervisor_stop(
                artifact_dir,
                grant=grant,
                reason="child_start_interrupted",
                child_exit_code=-1,
                terminate_sent=False,
                kill_sent=False,
                started_at=started_at,
                stopped_at=utc_now(),
            )
        else:
            _stop_and_reap(
                child,
                artifact_dir=artifact_dir,
                grant=grant,
                reason="supervisor_interrupted",
                started_at=started_at,
                cleanup_deadline=min(
                    plan.hard_stop_monotonic,
                    monotonic() + plan.termination_grace_seconds,
                ),
                monotonic=monotonic,
                utc_now=utc_now,
            )
        return 130
    except BaseException:
        _block_supervisor_signals()
        paths = _supervisor_paths(artifact_dir)
        terminal_started = any(
            paths[name].exists()
            for name in ("stop", "stop_manifest", "complete", "complete_manifest")
        )
        if child is None and not terminal_started:
            _write_supervisor_stop(
                artifact_dir,
                grant=grant,
                reason="child_start_failed",
                child_exit_code=-1,
                terminate_sent=False,
                kill_sent=False,
                started_at=started_at,
                stopped_at=utc_now(),
            )
        elif child is not None and not terminal_started:
            _stop_and_reap(
                child,
                artifact_dir=artifact_dir,
                grant=grant,
                reason="supervisor_failure",
                started_at=started_at,
                cleanup_deadline=min(
                    plan.hard_stop_monotonic,
                    monotonic() + plan.termination_grace_seconds,
                ),
                monotonic=monotonic,
                utc_now=utc_now,
            )
        raise
    finally:
        _block_supervisor_signals()
        try:
            if read_fd >= 0:
                _close_supervisor_fd(read_fd)
            if write_fd >= 0:
                _close_supervisor_fd(write_fd)
        finally:
            _restore_supervisor_signal_state(previous_handlers, prior_signal_mask)


def supervise_cli(raw_argv: list[str], config: RunConfig) -> int:
    """Run the live worker in a child bounded by the reviewed UTC cutoff."""
    validate_runtime_environment()
    plan = build_supervisor_plan(
        config,
        started_utc=datetime.now(UTC),
        started_monotonic=time.monotonic(),
    )
    nonce = secrets.token_hex(32)
    prior_signal_mask = _block_supervisor_signals()
    environment = os.environ.copy()
    environment["AWS_IGNORE_CONFIGURED_ENDPOINT_URLS"] = "true"
    command = [sys.executable, str(Path(__file__).resolve()), *raw_argv]
    claim: SupervisorClaim | None = None
    grant: SupervisorGrant | None = None
    try:
        claim = _claim_supervisor_evidence(config)
        grant = _build_supervisor_grant(config, claim, nonce=nonce)
        return _supervise_child(
            command,
            environment=environment,
            artifact_dir=config.artifact_dir.expanduser().resolve(),
            grant=grant,
            plan=plan,
            evidence_validator=lambda: _verify_evidence_binding(config, grant),
            prior_signal_mask=prior_signal_mask,
        )
    except BaseException:
        if claim is not None:
            if grant is None:
                grant = _build_supervisor_grant(config, claim, nonce=nonce)
            paths = _supervisor_paths(config.artifact_dir.expanduser().resolve())
            if not any(
                paths[name].exists()
                for name in ("stop", "stop_manifest", "complete", "complete_manifest")
            ):
                now = datetime.now(UTC)
                _write_supervisor_stop(
                    config.artifact_dir.expanduser().resolve(),
                    grant=grant,
                    reason="supervisor_setup_failed",
                    child_exit_code=-1,
                    terminate_sent=False,
                    kill_sent=False,
                    started_at=now,
                    stopped_at=now,
                )
        raise
    finally:
        _restore_signal_mask(prior_signal_mask)


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    args = parser.parse_args(raw_argv)
    try:
        config = config_from_args(args)
        workload = load_workload(args.workload)
    except (OSError, ValueError) as error:
        parser.error(str(error))

    if "HM_W5_SUPERVISOR_FD" not in os.environ:
        try:
            return supervise_cli(raw_argv, config)
        except (FileExistsError, OSError, ValueError) as error:
            parser.error(str(error))

    try:
        grant, capability_reader = _consume_supervisor_grant(config)
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as error:
        parser.error(str(error))

    stop_event = threading.Event()
    worker_finished = threading.Event()

    def request_stop(_signum: int, _frame: Any) -> None:
        stop_event.set()
        raise KeyboardInterrupt

    previous_sigterm = signal.signal(signal.SIGTERM, request_stop)
    watcher = threading.Thread(
        target=_watch_parent_liveness,
        args=(capability_reader, stop_event, worker_finished),
        daemon=True,
        name="hm-w5-supervisor-liveness",
    )
    watcher.start()
    try:
        _unblock_supervisor_signals()

        def live_transport(url: str, region: str, body: bytes, timeout: float) -> RawHttpResult:
            return signed_http_request(
                url,
                region,
                body,
                timeout,
                supervisor_grant=grant,
            )

        summary, exit_code = execute_run(
            config,
            workload,
            live_confirmation=args.confirm_live,
            transport=live_transport,
            stop_event=stop_event,
            supervisor_grant=grant,
        )
    except FileExistsError:
        parser.error(f"artifact-dir already exists; use a fresh path: {config.artifact_dir}")
    except ValueError as error:
        parser.error(str(error))
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)
        worker_finished.set()
        capability_reader.close()
        global _ACTIVE_SUPERVISOR_GRANT
        _ACTIVE_SUPERVISOR_GRANT = None

    print(f"summary: {config.artifact_dir.expanduser().resolve() / 'summary.json'}")
    print(
        f"status={summary['status']} valid_for_measurement={summary['valid_for_measurement']} "
        f"targets_met={summary['targets_met']}"
    )
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
