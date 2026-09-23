"""Offline regression tests for the Wave 5 signed serving load harness."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import signal
import stat
import subprocess
import sys
import threading
import time
import urllib.error
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from scripts import loadtest_signed_serving as loadtest

RUN_ID = "12345678-1234-4234-9234-123456789abc"
URL = "https://abc123.execute-api.us-east-1.amazonaws.com/v1/score-ais"
INTEGRATION_ID = "int123"
INTEGRATION_URI = "arn:aws:servicediscovery:us-east-1:123456789012:service/srv-wave5target"
FIXTURE = Path("bench/fixtures/wave5_score_request.json")
REAL_VALIDATE_REVIEWED_SOURCE = loadtest.validate_reviewed_source
REAL_VALIDATE_LIVE_OWNERSHIP = loadtest.validate_live_ownership


def trace_hex(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:32]


class FakeClock:
    def __init__(self) -> None:
        self.value = 0.0
        self.sleeps: list[float] = []
        self._lock = threading.Lock()

    def monotonic(self) -> float:
        with self._lock:
            return self.value

    def sleep(self, seconds: float) -> None:
        assert seconds >= 0
        with self._lock:
            self.sleeps.append(seconds)
            self.value += seconds

    def advance(self, seconds: float) -> None:
        with self._lock:
            self.value += seconds

    def utc_now(self) -> datetime:
        return datetime(2026, 8, 5, tzinfo=UTC) + timedelta(seconds=self.monotonic())


class ImmediateExecutor:
    def __init__(self, _workers: int) -> None:
        self.shutdown_calls: list[tuple[bool, bool]] = []

    def submit(self, fn, /, *args, **kwargs) -> Future:
        future: Future = Future()
        try:
            future.set_result(fn(*args, **kwargs))
        except BaseException as error:
            future.set_exception(error)
        return future

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        self.shutdown_calls.append((wait, cancel_futures))


def score_body(
    mmsi: int,
    trace_id: str,
    *,
    latency_ms: float = 12.5,
    hitl_required: bool = False,
    reasons: list[dict] | None = None,
) -> bytes:
    return json.dumps(
        {
            "mmsi": mmsi,
            "score": 0.1,
            "confidence": 0.9,
            "reasons": reasons or [],
            "hitl_required": hitl_required,
            "trace_id": trace_hex(trace_id),
            "latency_ms": latency_ms,
            "n_history": 1,
        }
    ).encode()


def unique_success_transport(clock: FakeClock, mmsi: int = 200000099):
    calls = {"count": 0}

    def transport(url: str, region: str, body: bytes, timeout: float):
        assert url == URL
        assert region == "us-east-1"
        assert json.loads(body)["mmsi"] == mmsi
        assert timeout == 2.0
        calls["count"] += 1
        clock.advance(0.01)
        return loadtest.RawHttpResult(
            200,
            score_body(mmsi, f"trace-{calls['count']}"),
        )

    return transport, calls


def config(tmp_path: Path, **overrides) -> loadtest.RunConfig:
    values = {
        "kind": "load",
        "run_id": RUN_ID,
        "api_url": URL,
        "region": "us-east-1",
        "expected_api_id": "abc123",
        "expected_integration_id": INTEGRATION_ID,
        "expected_integration_uri": INTEGRATION_URI,
        "expected_git_head": "a" * 40,
        "expected_harness_sha256": "b" * 64,
        "expected_window_logic_sha256": "c" * 64,
        "expected_models_sha256": "d" * 64,
        "operator_cutoff_utc": "2026-08-05T04:00:00Z",
        "artifact_dir": tmp_path / "run",
        "target_rps": 2.0,
        "max_in_flight": 2,
        "warmup_seconds": 1.0,
        "trial_seconds": 1.0,
        "trials": 3,
        "cooldown_seconds": 0.0,
        "request_timeout_seconds": 2.0,
        "drain_timeout_seconds": 3.0,
        "goodput_max_scheduled_response_ms": 100.0,
        "max_schedule_lag_ms": 50.0,
        "max_total_requests": 20,
        "minimum_success_ratio": 1.0,
        "minimum_goodput_ratio": 1.0,
    }
    values.update(overrides)
    return loadtest.RunConfig(**values)


def valid_argv(tmp_path: Path) -> list[str]:
    return [
        "--kind",
        "load",
        "--api-url",
        URL,
        "--region",
        "us-east-1",
        "--expected-api-id",
        "abc123",
        "--expected-integration-id",
        INTEGRATION_ID,
        "--expected-integration-uri",
        INTEGRATION_URI,
        "--expected-git-head",
        "a" * 40,
        "--expected-harness-sha256",
        "b" * 64,
        "--expected-window-logic-sha256",
        "c" * 64,
        "--expected-models-sha256",
        "d" * 64,
        "--operator-cutoff-utc",
        "2026-08-05T04:00:00Z",
        "--run-id",
        RUN_ID,
        "--artifact-dir",
        str(tmp_path / "run"),
        "--workload",
        str(FIXTURE),
        "--target-rps",
        "2",
        "--max-in-flight",
        "2",
        "--warmup-seconds",
        "1",
        "--trial-seconds",
        "1",
        "--trials",
        "3",
        "--cooldown-seconds",
        "0",
        "--request-timeout-seconds",
        "2",
        "--drain-timeout-seconds",
        "3",
        "--goodput-max-scheduled-response-ms",
        "100",
        "--max-schedule-lag-ms",
        "50",
        "--max-total-requests",
        "20",
        "--minimum-success-ratio",
        "1",
        "--minimum-goodput-ratio",
        "1",
        "--confirm-live",
        loadtest.CONFIRM_PHRASE,
    ]


def supervisor_grant(tmp_path: Path) -> loadtest.SupervisorGrant:
    return loadtest.SupervisorGrant(
        schema_version=loadtest.SCHEMA_VERSION,
        nonce="e" * 64,
        parent_pid=os.getppid(),
        run_id=RUN_ID,
        expected_api_id="abc123",
        expected_integration_id=INTEGRATION_ID,
        expected_integration_uri=INTEGRATION_URI,
        expected_git_head="a" * 40,
        expected_harness_sha256="b" * 64,
        expected_window_logic_sha256="c" * 64,
        expected_models_sha256="d" * 64,
        artifact_dir=str((tmp_path / "run").resolve()),
        operator_cutoff_utc="2026-08-05T04:00:00Z",
        claim_id="87654321-4321-4321-8321-cba987654321",
        claim_filename="run.supervisor-claim.json",
        claim_sha256="f" * 64,
    )


@pytest.fixture
def active_grant(tmp_path, monkeypatch):
    grant = supervisor_grant(tmp_path)
    monkeypatch.setattr(loadtest, "_ACTIVE_SUPERVISOR_GRANT", grant)
    return grant


@pytest.fixture(autouse=True)
def isolate_live_guards(monkeypatch):
    for name in (
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
    ):
        monkeypatch.delenv(name, raising=False)
    for name in list(os.environ):
        if name == "AWS_ENDPOINT_URL" or name.startswith("AWS_ENDPOINT_URL_"):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(
        loadtest,
        "validate_reviewed_source",
        lambda _config: {"git_head": "a" * 40, "worktree_clean": True, "sha256": {}},
    )
    monkeypatch.setattr(
        loadtest,
        "validate_live_ownership",
        lambda cfg: {
            "account": loadtest.EXPECTED_ACCOUNT_ID,
            "caller_arn": ("arn:aws:sts::123456789012:assumed-role/harbormaster-platform/test"),
            "api_id": "abc123",
            "route": {
                "route_id": "route-1",
                "route_key": loadtest.EXPECTED_ROUTE_KEY,
                "authorization_type": loadtest.EXPECTED_ROUTE_AUTHORIZATION,
                "target": f"integrations/{cfg.expected_integration_id}",
            },
            "integration": {
                "integration_id": cfg.expected_integration_id,
                "integration_uri": cfg.expected_integration_uri,
                "integration_type": loadtest.EXPECTED_INTEGRATION_TYPE,
                "integration_method": loadtest.EXPECTED_INTEGRATION_METHOD,
                "connection_type": loadtest.EXPECTED_CONNECTION_TYPE,
            },
        },
    )


def execute_run(cfg, workload, **kwargs):
    return loadtest.execute_run(
        cfg,
        workload,
        live_confirmation=loadtest.CONFIRM_PHRASE,
        **kwargs,
    )


def test_schedule_has_exact_absolute_offsets_and_phase_boundary():
    warmup = loadtest.Phase("warmup", None, 1.0)
    trial = loadtest.Phase("trial", 1, 1.0)

    warmup_schedule = loadtest.schedule_phase(warmup, 2.0, first_request_ordinal=1)
    trial_schedule = loadtest.schedule_phase(trial, 2.0, first_request_ordinal=3)

    assert [request.scheduled_offset_seconds for request in warmup_schedule] == [0.0, 0.5]
    assert [request.phase for request in warmup_schedule] == ["warmup", "warmup"]
    assert [request.scheduled_offset_seconds for request in trial_schedule] == [0.0, 0.5]
    assert [request.phase for request in trial_schedule] == ["trial", "trial"]
    assert [request.request_ordinal for request in warmup_schedule + trial_schedule] == [1, 2, 3, 4]


@pytest.mark.parametrize(
    ("duration", "rate", "expected"),
    [(2.0, 2.5, 5), (1.1, 2.0, 2), (0.49, 2.0, 0), (0.5, 2.0, 1)],
)
def test_fractional_request_count_is_floor_of_absolute_rate(duration, rate, expected):
    assert loadtest.request_count(duration, rate) == expected


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("target_rps", 0.0, "target_rps"),
        ("target_rps", float("nan"), "target_rps"),
        ("target_rps", 50.1, "target_rps"),
        ("max_in_flight", 0, "max_in_flight"),
        ("request_timeout_seconds", float("inf"), "request_timeout_seconds"),
        ("drain_timeout_seconds", 1.0, "drain_timeout_seconds"),
        ("minimum_success_ratio", 1.1, "minimum_success_ratio"),
        ("minimum_goodput_ratio", -0.1, "minimum_goodput_ratio"),
        ("max_total_requests", 8, "planned client request count"),
    ],
)
def test_config_rejects_invalid_controls(tmp_path, field, value, message):
    with pytest.raises(ValueError, match=message):
        config(tmp_path, **{field: value})


def test_max_total_requests_includes_the_preflight(tmp_path):
    with pytest.raises(ValueError, match="including preflight"):
        config(tmp_path, max_total_requests=8)
    cfg = config(tmp_path, max_total_requests=9)
    assert cfg.planned_request_count == 8
    assert cfg.planned_total_client_requests == 9


def test_load_and_soak_trial_policies_are_distinct(tmp_path):
    with pytest.raises(ValueError, match="at least 3"):
        config(tmp_path, trials=2)
    with pytest.raises(ValueError, match="exactly 1"):
        config(tmp_path, kind="soak", trials=3, trial_seconds=3600, target_rps=0.01)
    with pytest.raises(ValueError, match="trial_seconds >= 3600"):
        config(tmp_path, kind="soak", trials=1, trial_seconds=3599, target_rps=0.01)


def test_config_rejects_invalid_kind_run_id_empty_trial_and_time_ceiling(tmp_path):
    with pytest.raises(ValueError, match="kind"):
        config(tmp_path, kind="unknown")
    with pytest.raises(ValueError, match="canonical UUID4"):
        config(tmp_path, run_id="not-a-uuid")
    with pytest.raises(ValueError, match="canonical UUID4"):
        config(tmp_path, run_id="12345678-1234-1234-1234-123456789abc")
    with pytest.raises(ValueError, match="at least one request"):
        config(tmp_path, target_rps=0.1, trial_seconds=1.0)
    with pytest.raises(ValueError, match="exceed 14400"):
        config(tmp_path, warmup_seconds=10_000, trial_seconds=2_000, cooldown_seconds=1_000)


def test_config_rejects_non_us_east_1_even_with_matching_url(tmp_path):
    west_url = "https://abc123.execute-api.us-west-2.amazonaws.com/v1/score-ais"
    with pytest.raises(ValueError, match="region must equal us-east-1"):
        config(tmp_path, region="us-west-2", api_url=west_url)


def test_supervisor_plan_uses_one_exact_monotonic_feasibility_equation(tmp_path):
    started = datetime(2026, 8, 5, tzinfo=UTC)
    base = config(tmp_path)
    required = base.required_supervised_window_seconds
    cfg = replace(
        base,
        operator_cutoff_utc=(started + timedelta(seconds=required)).isoformat(),
    )
    plan = loadtest.build_supervisor_plan(
        cfg,
        started_utc=started,
        started_monotonic=100.0,
    )
    assert plan.required_window_seconds == required
    assert plan.hard_stop_monotonic - plan.graceful_stop_monotonic == pytest.approx(
        cfg.termination_grace_seconds
    )

    too_short = replace(
        cfg,
        operator_cutoff_utc=(started + timedelta(seconds=required - 0.001)).isoformat(),
    )
    with pytest.raises(ValueError, match="reviewed supervised window"):
        loadtest.build_supervisor_plan(
            too_short,
            started_utc=started,
            started_monotonic=100.0,
        )


def test_phases_include_cooldowns_and_can_omit_warmup(tmp_path):
    cfg = config(tmp_path, warmup_seconds=0.0, cooldown_seconds=0.25)
    assert [(phase.name, phase.trial_index) for phase in cfg.phases()] == [
        ("trial", 1),
        ("cooldown", 1),
        ("trial", 2),
        ("cooldown", 2),
        ("trial", 3),
    ]
    assert (
        loadtest.schedule_phase(loadtest.Phase("cooldown", 1, 1.0), 2.0, first_request_ordinal=1)
        == []
    )


@pytest.mark.parametrize(
    "url",
    [
        "http://abc.execute-api.us-east-1.amazonaws.com/v1/score-ais",
        "https://abc.execute-api.us-west-2.amazonaws.com/v1/score-ais",
        "https://abc.execute-api.us-east-1.amazonaws.com/healthz",
        "https://abc.execute-api.us-east-1.amazonaws.com/v1/score-ais?debug=1",
        "https://user@abc.execute-api.us-east-1.amazonaws.com/v1/score-ais",
    ],
)
def test_config_rejects_non_exact_target(tmp_path, url):
    with pytest.raises(ValueError, match="exact regional HTTPS"):
        config(tmp_path, api_url=url)


def test_workload_is_schema_validated_and_canonicalized(tmp_path):
    workload = loadtest.load_workload(FIXTURE)
    parsed = json.loads(workload.body)

    assert workload.identifier == loadtest.WORKLOAD_ID
    assert workload.mmsi == 200000099
    assert parsed["mmsi"] == workload.mmsi
    assert workload.sha256 == hashlib.sha256(workload.body).hexdigest()
    assert b"\n" not in workload.body

    invalid = tmp_path / "invalid.json"
    invalid.write_text('{"mmsi":"not-an-int"}')
    with pytest.raises(ValueError, match="valid AisScoreIn"):
        loadtest.load_workload(invalid)

    oversized = tmp_path / "oversized.json"
    oversized.write_bytes(b" " * (64 * 1024 + 1))
    with pytest.raises(ValueError, match="<= 65536"):
        loadtest.load_workload(oversized)

    alternate = tmp_path / "alternate.json"
    alternate_payload = json.loads(FIXTURE.read_text())
    alternate_payload["mmsi"] += 1
    alternate.write_text(json.dumps(alternate_payload))
    with pytest.raises(ValueError, match="pinned wave5_score_request"):
        loadtest.load_workload(alternate)


def test_percentiles_match_existing_linear_interpolation():
    values = [50.0, 10.0, 30.0, 20.0, 40.0]
    assert loadtest._percentile(values, 0.50) == 30.0
    assert loadtest._percentile(values, 0.95) == pytest.approx(48.0)
    assert loadtest._percentile(values, 0.99) == pytest.approx(49.6)
    assert loadtest._percentile([], 0.95) is None
    assert loadtest._percentile(values, 0) == 10.0
    assert loadtest._percentile(values, 1) == 50.0
    assert loadtest._distribution([])["p95"] is None


def test_request_record_classifies_success_and_keeps_latency_surfaces_separate(tmp_path):
    clock = FakeClock()
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)
    transport, calls = unique_success_transport(clock)
    scheduled = loadtest.ScheduledRequest(1, "trial", 1, 1, 0.0)

    row = loadtest._request_record(
        scheduled,
        request_id="request-1",
        phase_started_monotonic=0.0,
        phase_end_monotonic=1.0,
        config=cfg,
        workload=workload,
        transport=transport,
        monotonic=clock.monotonic,
        utc_now=clock.utc_now,
        stop_event=threading.Event(),
    )

    assert calls["count"] == 1
    assert row["outcome"] == "success"
    assert row["response_contract_valid"] is True
    assert row["client_latency_ms"] == pytest.approx(10.0)
    assert row["server_latency_ms"] == 12.5
    assert row["trace_id"] == trace_hex("trace-1")
    assert row["good"] is True


def test_worker_rechecks_phase_boundary_before_transport(tmp_path):
    clock = FakeClock()
    clock.advance(0.2)
    cfg = config(tmp_path, max_schedule_lag_ms=1000.0)
    workload = loadtest.load_workload(FIXTURE)
    calls = {"count": 0}

    def transport(*_args):
        calls["count"] += 1
        raise AssertionError("late slot must not reach transport")

    row = loadtest._request_record(
        loadtest.ScheduledRequest(1, "trial", 1, 1, 0.0),
        request_id="request-late",
        phase_started_monotonic=0.0,
        phase_end_monotonic=0.1,
        config=cfg,
        workload=workload,
        transport=transport,
        monotonic=clock.monotonic,
        utc_now=clock.utc_now,
        stop_event=threading.Event(),
    )

    assert calls["count"] == 0
    assert row["outcome"] == "schedule_lag_drop"
    assert row["started_at_utc"] is None


def test_goodput_uses_scheduled_arrival_to_completion(tmp_path):
    clock = FakeClock()
    clock.advance(0.04)
    cfg = config(
        tmp_path,
        max_schedule_lag_ms=100.0,
        goodput_max_scheduled_response_ms=50.0,
    )
    workload = loadtest.load_workload(FIXTURE)

    def transport(*_args):
        clock.advance(0.04)
        return loadtest.RawHttpResult(200, score_body(workload.mmsi, "scheduled-latency"))

    row = loadtest._request_record(
        loadtest.ScheduledRequest(1, "trial", 1, 1, 0.0),
        request_id="request-latency",
        phase_started_monotonic=0.0,
        phase_end_monotonic=1.0,
        config=cfg,
        workload=workload,
        transport=transport,
        monotonic=clock.monotonic,
        utc_now=clock.utc_now,
        stop_event=threading.Event(),
    )

    assert row["outcome"] == "success"
    assert row["client_latency_ms"] == pytest.approx(40.0)
    assert row["scheduled_to_completion_ms"] == pytest.approx(80.0)
    assert row["good"] is False


@pytest.mark.parametrize(
    ("result", "outcome", "error_code"),
    [
        (loadtest.RawHttpResult(None, b"", "timeout"), "transport_error", "timeout"),
        (loadtest.RawHttpResult(429, b"rate limited"), "http_4xx", "http_429"),
        (loadtest.RawHttpResult(503, b"unavailable"), "http_5xx", "http_503"),
        (loadtest.RawHttpResult(302, b"redirect"), "http_other", "http_302"),
        (loadtest.RawHttpResult(200, b"not-json"), "contract_error", "invalid_ais_score_response"),
    ],
)
def test_request_failures_are_classified(tmp_path, result, outcome, error_code):
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)
    scheduled = loadtest.ScheduledRequest(1, "trial", 1, 1, 0.0)

    row = loadtest._request_record(
        scheduled,
        request_id="request-1",
        phase_started_monotonic=0.0,
        phase_end_monotonic=1.0,
        config=cfg,
        workload=workload,
        transport=lambda *_args: result,
        monotonic=lambda: 0.0,
        utc_now=lambda: datetime(2026, 8, 5, tzinfo=UTC),
        stop_event=threading.Event(),
    )

    assert row["outcome"] == outcome
    assert row["error_code"] == error_code
    assert row["good"] is False


def test_request_transport_exception_is_sanitized(tmp_path):
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)
    scheduled = loadtest.ScheduledRequest(1, "trial", 1, 1, 0.0)

    def transport(*_args):
        raise RuntimeError("Authorization: secret-value")

    row = loadtest._request_record(
        scheduled,
        request_id="request-1",
        phase_started_monotonic=0.0,
        phase_end_monotonic=1.0,
        config=cfg,
        workload=workload,
        transport=transport,
        monotonic=lambda: 0.0,
        utc_now=lambda: datetime(2026, 8, 5, tzinfo=UTC),
        stop_event=threading.Event(),
    )
    assert row["outcome"] == "transport_error"
    assert row["error_code"] == "transport_exception"
    assert "secret" not in json.dumps(row)


def test_mismatched_mmsi_is_a_contract_error(tmp_path):
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)
    scheduled = loadtest.ScheduledRequest(1, "trial", 1, 1, 0.0)
    row = loadtest._request_record(
        scheduled,
        request_id="request-1",
        phase_started_monotonic=0.0,
        phase_end_monotonic=1.0,
        config=cfg,
        workload=workload,
        transport=lambda *_args: loadtest.RawHttpResult(200, score_body(111000111, "trace")),
        monotonic=lambda: 0.0,
        utc_now=lambda: datetime(2026, 8, 5, tzinfo=UTC),
        stop_event=threading.Event(),
    )
    assert row["outcome"] == "contract_error"
    assert row["response_contract_valid"] is False


def test_absolute_pacing_does_not_accumulate_request_latency(tmp_path):
    clock = FakeClock()
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)
    transport, calls = unique_success_transport(clock)
    ledger = loadtest.EventLedger(tmp_path / "events.jsonl", RUN_ID)
    request_ids = iter(f"request-{index}" for index in range(1, 20))

    records, status, reason = loadtest.run_open_loop(
        cfg,
        workload,
        ledger,
        transport=transport,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.utc_now,
        request_id_factory=lambda: next(request_ids),
        executor_factory=ImmediateExecutor,
    )
    ledger.close()

    assert status == "completed"
    assert reason is None
    assert calls["count"] == cfg.planned_request_count
    assert len(records) == cfg.planned_request_count
    assert all(row["outcome"] == "success" for row in records)
    assert [
        row["request_ordinal"] for row in sorted(records, key=lambda row: row["request_ordinal"])
    ] == list(range(1, cfg.planned_request_count + 1))
    assert clock.sleeps
    assert max(clock.sleeps) <= 0.5
    assert clock.monotonic() == pytest.approx(4.0)


def test_late_scheduler_drops_slots_without_catch_up(tmp_path):
    clock = FakeClock()
    cfg = config(tmp_path, max_schedule_lag_ms=50.0)
    workload = loadtest.load_workload(FIXTURE)
    calls = {"count": 0}

    def slow_transport(*_args):
        calls["count"] += 1
        clock.advance(1.0)
        return loadtest.RawHttpResult(200, score_body(workload.mmsi, f"trace-{calls['count']}"))

    ledger = loadtest.EventLedger(tmp_path / "late-events.jsonl", RUN_ID)
    records, status, reason = loadtest.run_open_loop(
        cfg,
        workload,
        ledger,
        transport=slow_transport,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.utc_now,
        executor_factory=ImmediateExecutor,
    )
    ledger.close()

    assert status == "completed"
    assert reason is None
    assert {row["outcome"] for row in records} == {"success", "schedule_lag_drop"}
    assert calls["count"] == 4
    assert len(records) == cfg.planned_request_count


def test_real_threads_enforce_inflight_cap_and_shed_without_replay(tmp_path):
    clock = FakeClock()
    cfg = config(
        tmp_path,
        warmup_seconds=0.0,
        max_in_flight=1,
        max_schedule_lag_ms=1000.0,
        goodput_max_scheduled_response_ms=2000.0,
    )
    workload = loadtest.load_workload(FIXTURE)
    condition = threading.Condition()
    gates: list[threading.Event] = []
    active = 0
    max_active = 0

    def transport(*_args):
        nonlocal active, max_active
        gate = threading.Event()
        with condition:
            gates.append(gate)
            active += 1
            max_active = max(max_active, active)
            condition.notify_all()
            call_index = len(gates)
        assert gate.wait(timeout=2.0)
        with condition:
            active -= 1
        return loadtest.RawHttpResult(
            200,
            score_body(workload.mmsi, f"threaded-{call_index}"),
        )

    sleep_calls = 0

    def controlled_sleep(seconds: float):
        nonlocal sleep_calls
        sleep_calls += 1
        phase_index = (sleep_calls - 1) // 2
        if sleep_calls % 2 == 1:
            with condition:
                assert condition.wait_for(
                    lambda: len(gates) > phase_index,
                    timeout=2.0,
                )
            clock.sleep(seconds)
        else:
            clock.sleep(seconds)
            gates[phase_index].set()

    ledger = loadtest.EventLedger(tmp_path / "threaded-events.jsonl", RUN_ID)
    records, status, reason = loadtest.run_open_loop(
        cfg,
        workload,
        ledger,
        transport=transport,
        monotonic=clock.monotonic,
        sleep=controlled_sleep,
        utc_now=clock.utc_now,
        executor_factory=lambda workers: ThreadPoolExecutor(max_workers=workers),
    )
    ledger.close()

    assert status == "completed"
    assert reason is None
    assert max_active == 1
    assert len(gates) == 3
    assert len(records) == 6
    assert [row["outcome"] for row in records].count("client_overload_drop") == 3
    assert [row["outcome"] for row in records].count("success") == 3
    assert sorted(row["request_ordinal"] for row in records) == list(range(1, 7))

    events = [
        json.loads(line) for line in (tmp_path / "threaded-events.jsonl").read_text().splitlines()
    ]
    for trial_index in range(1, 4):
        request_sequences = [
            event["event_seq"]
            for event in events
            if event["record_type"] == "request" and event["trial_index"] == trial_index
        ]
        phase_end_sequence = next(
            event["event_seq"]
            for event in events
            if event["record_type"] == "phase_ended" and event["trial_index"] == trial_index
        )
        assert max(request_sequences) < phase_end_sequence


def test_drain_timeout_is_recorded_and_stops_later_phases(tmp_path):
    cfg = config(
        tmp_path,
        warmup_seconds=0.0,
        target_rps=10.0,
        trial_seconds=0.1,
        request_timeout_seconds=0.02,
        drain_timeout_seconds=0.02,
        max_in_flight=1,
        max_total_requests=4,
        max_schedule_lag_ms=1000.0,
    )
    workload = loadtest.load_workload(FIXTURE)
    release = threading.Event()
    timer = threading.Timer(0.15, release.set)
    calls = {"count": 0}

    def transport(*_args):
        calls["count"] += 1
        assert release.wait(timeout=1.0)
        return loadtest.RawHttpResult(200, score_body(workload.mmsi, "late-drain"))

    ledger = loadtest.EventLedger(tmp_path / "drain-events.jsonl", RUN_ID)
    timer.start()
    try:
        records, status, reason = loadtest.run_open_loop(
            cfg,
            workload,
            ledger,
            transport=transport,
            executor_factory=lambda workers: ThreadPoolExecutor(max_workers=workers),
        )
    finally:
        timer.cancel()
        ledger.close()

    assert status == "failed"
    assert reason == "drain_timeout"
    assert calls["count"] == 1
    assert len(records) == 1
    events = [
        json.loads(line) for line in (tmp_path / "drain-events.jsonl").read_text().splitlines()
    ]
    detected = [event for event in events if event["record_type"] == "drain_timeout_detected"]
    assert len(detected) == 1
    assert detected[0]["pending_futures"] == 1
    assert not any(
        event.get("trial_index") in {2, 3} and event["record_type"] == "phase_started"
        for event in events
    )


def test_request_event_write_failure_has_terminal_precedence(tmp_path, monkeypatch):
    clock = FakeClock()
    cfg = config(tmp_path, warmup_seconds=0.0)
    workload = loadtest.load_workload(FIXTURE)
    transport, _calls = unique_success_transport(clock)
    ledger = loadtest.EventLedger(tmp_path / "write-failure.jsonl", RUN_ID)
    original_write = ledger.write

    def failing_write(record_type, **fields):
        if record_type == "request":
            raise OSError("simulated request ledger failure")
        return original_write(record_type, **fields)

    monkeypatch.setattr(ledger, "write", failing_write)
    records, status, reason = loadtest.run_open_loop(
        cfg,
        workload,
        ledger,
        transport=transport,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.utc_now,
        executor_factory=ImmediateExecutor,
    )
    ledger.close()

    assert records == []
    assert status == "failed"
    assert reason == "event_write_failure"


def test_stop_event_prevents_any_phase_from_starting(tmp_path):
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)
    ledger = loadtest.EventLedger(tmp_path / "stopped.jsonl", RUN_ID)
    stopped = threading.Event()
    stopped.set()
    records, status, reason = loadtest.run_open_loop(
        cfg,
        workload,
        ledger,
        transport=lambda *_args: (_ for _ in ()).throw(AssertionError("must not run")),
        executor_factory=ImmediateExecutor,
        stop_event=stopped,
    )
    ledger.close()
    assert records == []
    assert status == "interrupted"
    assert reason == "stop_requested"


def test_summary_excludes_warmup_and_includes_drops_in_goodput_denominator(tmp_path):
    base = {
        "record_type": "request",
        "started_at_utc": "2026-08-05T00:00:00Z",
        "client_latency_ms": 10.0,
        "server_latency_ms": 5.0,
        "http_status": 200,
        "outcome": "success",
        "good": True,
        "trace_id": "trace",
        "schedule_lag_ms": 0.0,
        "scheduled_offset_seconds": 0.0,
    }
    warmup = {**base, "phase": "warmup", "trial_index": None, "request_ordinal": 1}
    success = {**base, "phase": "trial", "trial_index": 1, "request_ordinal": 2}
    dropped = {
        **base,
        "phase": "trial",
        "trial_index": 1,
        "request_ordinal": 3,
        "started_at_utc": None,
        "client_latency_ms": None,
        "server_latency_ms": None,
        "http_status": None,
        "outcome": "client_overload_drop",
        "good": False,
        "trace_id": None,
    }

    population = loadtest.summarize_population([success, dropped], 1.0)
    assert population["scheduled"] == 2
    assert population["valid_http_200"] == 1
    assert population["goodput_ratio"] == 0.5
    assert population["client_latency_ms_valid_success"]["sample_count"] == 1
    assert loadtest.summarize_population([warmup], 1.0)["valid_http_200"] == 1


def test_execute_run_writes_checksum_bound_secret_free_evidence(tmp_path):
    clock = FakeClock()
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)
    transport, calls = unique_success_transport(clock)
    request_ids = iter(f"request-{index}" for index in range(1, 20))

    summary, exit_code = execute_run(
        cfg,
        workload,
        transport=transport,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.utc_now,
        request_id_factory=lambda: next(request_ids),
        executor_factory=ImmediateExecutor,
    )

    assert exit_code == 0
    assert calls["count"] == 1 + cfg.planned_request_count
    assert summary["status"] == "completed"
    assert summary["valid_for_measurement"] is True
    assert summary["targets_met"] is True
    assert summary["accounting"]["warmup_request_events"] == 2
    assert summary["preflight"]["attempted"] is True
    assert summary["preflight"]["client_outcome_known"] is True
    assert summary["accounting"]["preflight_attempts"] == 1
    assert summary["accounting"]["actual_client_attempts_including_preflight"] == (
        1 + cfg.planned_request_count
    )
    assert summary["accounting"]["measured_request_events"] == 6
    assert summary["billing"]["status"] == "not_collected"

    expected_files = {
        "events.jsonl",
        "evidence-files.sha256",
        "run-spec.json",
        "summary.json",
        "workload.json",
    }
    assert {path.name for path in cfg.artifact_dir.iterdir()} == expected_files
    assert stat.S_IMODE(cfg.artifact_dir.stat().st_mode) == 0o700
    for path in cfg.artifact_dir.iterdir():
        assert stat.S_IMODE(path.stat().st_mode) == 0o600

    manifest = (cfg.artifact_dir / "evidence-files.sha256").read_text().splitlines()
    assert [line.split("  ", 1)[1] for line in manifest] == [
        "events.jsonl",
        "run-spec.json",
        "summary.json",
        "workload.json",
    ]
    for line in manifest:
        digest, name = line.split("  ", 1)
        assert hashlib.sha256((cfg.artifact_dir / name).read_bytes()).hexdigest() == digest

    serialized = "\n".join(path.read_text() for path in cfg.artifact_dir.iterdir())
    for forbidden in (
        "Authorization",
        "X-Amz-Security-Token",
        "AWS_SECRET_ACCESS_KEY",
        "session-token",
    ):
        assert forbidden not in serialized

    events = [
        json.loads(line) for line in (cfg.artifact_dir / "events.jsonl").read_text().splitlines()
    ]
    assert [event["event_seq"] for event in events] == list(range(1, len(events) + 1))


def test_preflight_failure_stops_before_scheduled_load(tmp_path):
    clock = FakeClock()
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)
    calls = {"count": 0}

    def failing_transport(*_args):
        calls["count"] += 1
        return loadtest.RawHttpResult(403, b"forbidden")

    summary, exit_code = execute_run(
        cfg,
        workload,
        transport=failing_transport,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.utc_now,
        executor_factory=ImmediateExecutor,
    )

    assert exit_code == 1
    assert calls["count"] == 1
    assert summary["status"] == "failed"
    assert summary["stop_reason"] == "preflight_failed"
    assert summary["accounting"]["request_events"] == 0
    assert summary["valid_for_measurement"] is False


def test_unsafe_hitl_preflight_stops_before_scheduled_load(tmp_path):
    clock = FakeClock()
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)
    calls = {"count": 0}

    def unsafe_transport(*_args):
        calls["count"] += 1
        return loadtest.RawHttpResult(
            200,
            score_body(workload.mmsi, "unsafe-preflight", hitl_required=True),
        )

    summary, exit_code = execute_run(
        cfg,
        workload,
        transport=unsafe_transport,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.utc_now,
        executor_factory=ImmediateExecutor,
    )

    assert exit_code == 1
    assert calls["count"] == 1
    assert summary["preflight"]["outcome"] == "unsafe_stateful_response"
    assert summary["preflight"]["hitl_required"] is True
    assert summary["accounting"]["request_events"] == 0


def test_first_unsafe_measured_response_stops_remaining_schedule(tmp_path):
    clock = FakeClock()
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)
    calls = {"count": 0}

    def transport(*_args):
        calls["count"] += 1
        hitl = calls["count"] == 2
        return loadtest.RawHttpResult(
            200,
            score_body(workload.mmsi, f"unsafe-{calls['count']}", hitl_required=hitl),
        )

    summary, exit_code = execute_run(
        cfg,
        workload,
        transport=transport,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.utc_now,
        executor_factory=ImmediateExecutor,
    )

    assert exit_code == 1
    assert calls["count"] == 2
    assert summary["status"] == "failed"
    assert summary["stop_reason"] == "unsafe_stateful_response"
    assert summary["accounting"]["unsafe_response_events"] == 1
    assert summary["valid_for_measurement"] is False


def test_async_unsafe_response_interrupts_scheduler_before_next_submission(tmp_path):
    cfg = config(
        tmp_path,
        warmup_seconds=0.0,
        target_rps=4.0,
        max_in_flight=2,
    )
    workload = loadtest.load_workload(FIXTURE)
    calls = {"count": 0}
    calls_lock = threading.Lock()

    def transport(*_args):
        with calls_lock:
            calls["count"] += 1
            call_number = calls["count"]
        if call_number == 2:
            time.sleep(0.05)
        return loadtest.RawHttpResult(
            200,
            score_body(
                workload.mmsi,
                f"async-{call_number}",
                hitl_required=call_number == 2,
            ),
        )

    summary, exit_code = execute_run(cfg, workload, transport=transport)

    assert exit_code == 1
    assert calls["count"] == 2
    assert summary["stop_reason"] == "unsafe_stateful_response"
    assert summary["accounting"]["unsafe_response_events"] == 1
    assert summary["valid_for_measurement"] is False


def test_duplicate_success_trace_ids_invalidate_measurement(tmp_path):
    clock = FakeClock()
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)

    def duplicate_transport(*_args):
        clock.advance(0.01)
        return loadtest.RawHttpResult(200, score_body(workload.mmsi, "duplicate-trace"))

    summary, exit_code = execute_run(
        cfg,
        workload,
        transport=duplicate_transport,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.utc_now,
        executor_factory=ImmediateExecutor,
    )

    assert exit_code == 1
    assert summary["trace_identity"]["duplicate_trace_ids"] == 8
    assert summary["valid_for_measurement"] is False


def test_cross_scope_duplicate_trace_id_invalidates_measurement(tmp_path):
    clock = FakeClock()
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)
    calls = {"count": 0}

    def transport(*_args):
        calls["count"] += 1
        clock.advance(0.01)
        trace = "cross-scope" if calls["count"] in {1, 2} else f"unique-{calls['count']}"
        return loadtest.RawHttpResult(200, score_body(workload.mmsi, trace))

    summary, exit_code = execute_run(
        cfg,
        workload,
        transport=transport,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.utc_now,
        executor_factory=ImmediateExecutor,
    )

    assert exit_code == 1
    assert summary["trace_identity"] == {
        "scope": "all_observed_trace_ids_across_preflight_warmup_and_measured_trials",
        "preflight_observed_trace_ids": 1,
        "warmup_observed_trace_ids": 2,
        "measured_observed_trace_ids": 6,
        "observed_trace_ids": 9,
        "unique_trace_ids": 8,
        "duplicate_trace_ids": 1,
        "all_observed_trace_ids_unique": False,
    }
    assert summary["valid_for_measurement"] is False


def test_soak_summary_includes_sixty_second_buckets(tmp_path):
    cfg = config(
        tmp_path,
        kind="soak",
        trials=1,
        warmup_seconds=0,
        trial_seconds=3600,
        target_rps=0.001,
        max_total_requests=10,
    )
    rows = []
    for index, offset in enumerate((0.0, 1000.0, 3599.0), start=1):
        rows.append(
            {
                "phase": "trial",
                "trial_index": 1,
                "request_id": f"request-{index}",
                "request_ordinal": index,
                "scheduled_offset_seconds": offset,
                "started_at_utc": "2026-08-05T00:00:00Z",
                "client_latency_ms": 10.0,
                "server_latency_ms": 5.0,
                "schedule_lag_ms": 0.0,
                "http_status": 200,
                "outcome": "success",
                "good": True,
                "trace_id": f"trace-{index}",
            }
        )
    summary = loadtest.build_summary(
        cfg,
        rows,
        terminal_status="completed",
        stop_reason=None,
        preflight={
            "attempted": True,
            "client_outcome_known": True,
            "outcome": "success",
            "trace_id": trace_hex("preflight"),
        },
        started_at=datetime(2026, 8, 5, tzinfo=UTC),
        ended_at=datetime(2026, 8, 5, 1, tzinfo=UTC),
        elapsed_seconds=3600,
        run_spec_sha256="a" * 64,
        events_sha256="b" * 64,
    )
    assert summary["valid_for_measurement"] is True
    assert len(summary["soak_60_second_buckets"]) == 60
    assert summary["soak_60_second_buckets"][1]["scheduled"] == 0


def test_artifact_directory_collision_is_refused_without_transport(tmp_path):
    cfg = config(tmp_path)
    cfg.artifact_dir.mkdir()
    sentinel = cfg.artifact_dir / "sentinel"
    sentinel.write_text("keep")
    workload = loadtest.load_workload(FIXTURE)
    calls = {"count": 0}

    def transport(*_args):
        calls["count"] += 1
        raise AssertionError("transport must not run")

    with pytest.raises(FileExistsError):
        execute_run(cfg, workload, transport=transport)
    assert calls["count"] == 0
    assert sentinel.read_text() == "keep"


def test_execution_boundary_rejects_missing_confirmation_before_guards_or_transport(
    tmp_path,
    monkeypatch,
):
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)
    calls = {"source": 0, "ownership": 0, "transport": 0}

    def source_guard(_config):
        calls["source"] += 1
        raise AssertionError("source guard must not run")

    def ownership_guard(_config):
        calls["ownership"] += 1
        raise AssertionError("ownership guard must not run")

    def transport(*_args):
        calls["transport"] += 1
        raise AssertionError("transport must not run")

    monkeypatch.setattr(loadtest, "validate_reviewed_source", source_guard)
    monkeypatch.setattr(loadtest, "validate_live_ownership", ownership_guard)
    with pytest.raises(ValueError, match=loadtest.CONFIRM_PHRASE):
        loadtest.execute_run(
            cfg,
            workload,
            live_confirmation="wrong",
            transport=transport,
        )
    assert calls == {"source": 0, "ownership": 0, "transport": 0}
    assert not cfg.artifact_dir.exists()


def test_live_transport_cannot_run_outside_process_supervisor(tmp_path, monkeypatch):
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)
    monkeypatch.setattr(loadtest, "_ACTIVE_SUPERVISOR_GRANT", None)
    with pytest.raises(ValueError, match="inherited supervisor grant"):
        loadtest.execute_run(
            cfg,
            workload,
            live_confirmation=loadtest.CONFIRM_PHRASE,
            transport=loadtest.signed_http_request,
        )
    assert not cfg.artifact_dir.exists()


def test_wrapper_cannot_bypass_signed_transport_supervisor_grant(tmp_path, monkeypatch):
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)
    monkeypatch.setattr(loadtest, "_ACTIVE_SUPERVISOR_GRANT", None)
    signed_calls = {"count": 0}

    def headers(*_args):
        signed_calls["count"] += 1
        raise AssertionError("signing must not begin")

    def wrapper(url, region, body, timeout):
        return loadtest.signed_http_request(url, region, body, timeout)

    monkeypatch.setattr(loadtest, "sigv4_headers", headers)
    summary, exit_code = execute_run(cfg, workload, transport=wrapper)
    assert exit_code == 1
    assert signed_calls["count"] == 0
    assert summary["preflight"]["outcome"] == "transport_error"
    assert summary["valid_for_measurement"] is False


def test_parser_requires_exact_live_confirmation(tmp_path):
    parser = loadtest.build_parser()
    argv = valid_argv(tmp_path)
    argv[-1] = "wrong"
    args = parser.parse_args(argv)
    with pytest.raises(ValueError, match=loadtest.CONFIRM_PHRASE):
        loadtest.config_from_args(args)
    assert not (tmp_path / "run").exists()


def test_signed_transport_signs_once_and_refuses_oversized_body(monkeypatch, active_grant):
    calls: list[tuple] = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @staticmethod
        def read(_size):
            return b"x" * (loadtest.MAX_RESPONSE_BYTES + 1)

    def headers(url, body, region):
        calls.append(("sign", url, body, region))
        return {"Authorization": "redacted"}

    def open_request(request, *, timeout_seconds):
        calls.append(("open", request.full_url, request.data, timeout_seconds))
        return Response()

    monkeypatch.setattr(loadtest, "sigv4_headers", headers)
    monkeypatch.setattr(loadtest, "open_no_redirect", open_request)

    result = loadtest.signed_http_request(
        URL,
        "us-east-1",
        b"{}",
        2.0,
        supervisor_grant=active_grant,
    )

    assert [call[0] for call in calls] == ["sign", "open"]
    assert result.status_code == 200
    assert result.body == b""
    assert result.error_code == "response_too_large"


@pytest.mark.parametrize(
    ("raised", "expected_code", "expected_status"),
    [
        (TimeoutError(), "timeout", None),
        (urllib.error.URLError(TimeoutError()), "timeout", None),
        (urllib.error.URLError("dns"), "url_error", None),
        (OSError(), "os_error", None),
    ],
)
def test_signed_transport_classifies_network_errors(
    monkeypatch, active_grant, raised, expected_code, expected_status
):
    monkeypatch.setattr(loadtest, "sigv4_headers", lambda *_args: {})
    monkeypatch.setattr(
        loadtest, "open_no_redirect", lambda *_args, **_kwargs: (_ for _ in ()).throw(raised)
    )
    result = loadtest.signed_http_request(
        URL,
        "us-east-1",
        b"{}",
        2.0,
        supervisor_grant=active_grant,
    )
    assert result.error_code == expected_code
    assert result.status_code == expected_status


def test_signed_transport_captures_http_error_without_retry(monkeypatch, active_grant):
    http_error = urllib.error.HTTPError(
        URL,
        429,
        "rate limited",
        {},
        io.BytesIO(b"limited"),
    )
    monkeypatch.setattr(loadtest, "sigv4_headers", lambda *_args: {})
    monkeypatch.setattr(
        loadtest,
        "open_no_redirect",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(http_error),
    )
    result = loadtest.signed_http_request(
        URL,
        "us-east-1",
        b"{}",
        2.0,
        supervisor_grant=active_grant,
    )
    assert result == loadtest.RawHttpResult(429, b"limited", None)


def test_keyboard_interrupt_seals_partial_evidence_and_returns_130(tmp_path):
    clock = FakeClock()
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)
    transport, _calls = unique_success_transport(clock)
    sleep_calls = {"count": 0}

    def interrupting_sleep(seconds: float):
        sleep_calls["count"] += 1
        if sleep_calls["count"] == 2:
            raise KeyboardInterrupt
        clock.sleep(seconds)

    summary, exit_code = execute_run(
        cfg,
        workload,
        transport=transport,
        monotonic=clock.monotonic,
        sleep=interrupting_sleep,
        utc_now=clock.utc_now,
        executor_factory=ImmediateExecutor,
    )

    assert exit_code == 130
    assert summary["status"] == "interrupted"
    assert summary["stop_reason"] == "keyboard_interrupt"
    assert summary["valid_for_measurement"] is False
    assert (cfg.artifact_dir / "evidence-files.sha256").is_file()
    events = [
        json.loads(line) for line in (cfg.artifact_dir / "events.jsonl").read_text().splitlines()
    ]
    persisted_requests = [event for event in events if event["record_type"] == "request"]
    assert persisted_requests
    assert summary["accounting"]["request_events"] == len(persisted_requests)


def test_interrupted_preflight_is_counted_as_attempted_with_unknown_outcome(tmp_path):
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)

    def interrupted_transport(*_args):
        raise KeyboardInterrupt

    summary, exit_code = execute_run(
        cfg,
        workload,
        transport=interrupted_transport,
    )

    assert exit_code == 130
    assert summary["preflight"]["attempted"] is True
    assert summary["preflight"]["client_outcome_known"] is False
    assert summary["preflight"]["outcome"] == "unknown"
    assert summary["accounting"]["preflight_attempts"] == 1
    assert summary["accounting"]["actual_client_attempts_including_preflight"] == 1
    assert summary["accounting"]["request_events"] == 0
    assert summary["valid_for_measurement"] is False
    event_types = [
        json.loads(line)["record_type"]
        for line in (cfg.artifact_dir / "events.jsonl").read_text().splitlines()
    ]
    assert "preflight_attempt_started" in event_types
    assert "preflight_completed" not in event_types


def test_evidence_files_never_use_world_readable_modes(tmp_path):
    path = tmp_path / "payload.json"
    loadtest._write_json_exclusive(path, {"ok": True})
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600


def test_event_ledger_enforces_total_byte_ceiling(tmp_path):
    ledger = loadtest.EventLedger(tmp_path / "bounded.jsonl", RUN_ID, max_bytes=1)
    with pytest.raises(RuntimeError, match="byte ceiling"):
        ledger.write("request", payload="too-large")
    ledger.close()
    assert (tmp_path / "bounded.jsonl").read_bytes() == b""


def test_runtime_environment_rejects_proxy_and_ca_overrides(monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.invalid")
    with pytest.raises(ValueError, match="HTTPS_PROXY"):
        loadtest.validate_runtime_environment()


@pytest.mark.parametrize(
    "name",
    (
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_STS",
        "AWS_ENDPOINT_URL_APIGATEWAYV2",
        "AWS_ENDPOINT_URL_ARBITRARY_SERVICE",
        "SSL_CERT_DIR",
    ),
)
def test_runtime_environment_rejects_endpoint_and_trust_overrides(monkeypatch, name):
    monkeypatch.setenv(name, "https://override.invalid")
    with pytest.raises(ValueError, match=name):
        loadtest.validate_runtime_environment()


def test_route_reader_is_bounded_and_rejects_repeated_tokens():
    class PaginatedClient:
        def __init__(self):
            self.calls = []

        def get_routes(self, **kwargs):
            self.calls.append(kwargs)
            if len(self.calls) == 1:
                return {"Items": [], "NextToken": "next"}
            return {
                "Items": [
                    {
                        "RouteId": "route-1",
                        "RouteKey": loadtest.EXPECTED_ROUTE_KEY,
                        "AuthorizationType": loadtest.EXPECTED_ROUTE_AUTHORIZATION,
                    }
                ]
            }

    client = PaginatedClient()
    routes = loadtest._read_api_routes(client, "abc123")
    assert len(routes) == 1
    assert client.calls[1]["NextToken"] == "next"

    class LoopingClient:
        @staticmethod
        def get_routes(**_kwargs):
            return {"Items": [], "NextToken": "same"}

    with pytest.raises(ValueError, match="pagination token"):
        loadtest._read_api_routes(LoopingClient(), "abc123")


def test_ownership_clients_use_official_endpoints_no_proxy_and_exact_route(
    tmp_path,
    monkeypatch,
):
    cfg = config(tmp_path)
    captured: dict[str, dict] = {}
    integration_calls: list[dict] = []

    class Meta:
        def __init__(self, endpoint_url):
            self.endpoint_url = endpoint_url

    class StsClient:
        def __init__(self, endpoint_url):
            self.meta = Meta(endpoint_url)

        @staticmethod
        def get_caller_identity():
            return {
                "Account": loadtest.EXPECTED_ACCOUNT_ID,
                "Arn": ("arn:aws:sts::123456789012:assumed-role/harbormaster-platform/w5-load"),
            }

    class ApiClient:
        def __init__(self, endpoint_url):
            self.meta = Meta(endpoint_url)

        @staticmethod
        def get_api(**_kwargs):
            return {
                "Name": loadtest.EXPECTED_API_NAME,
                "ProtocolType": "HTTP",
                "ApiEndpoint": "https://abc123.execute-api.us-east-1.amazonaws.com",
                "Tags": {
                    "Project": "harbormaster",
                    "Environment": "base",
                    "ManagedBy": "terraform",
                },
            }

        @staticmethod
        def get_routes(**_kwargs):
            return {
                "Items": [
                    {
                        "RouteId": "route-1",
                        "RouteKey": loadtest.EXPECTED_ROUTE_KEY,
                        "AuthorizationType": loadtest.EXPECTED_ROUTE_AUTHORIZATION,
                        "Target": f"integrations/{INTEGRATION_ID}",
                    }
                ]
            }

        @staticmethod
        def get_integration(**kwargs):
            integration_calls.append(kwargs)
            return {
                "IntegrationId": INTEGRATION_ID,
                "IntegrationUri": INTEGRATION_URI,
                "IntegrationType": loadtest.EXPECTED_INTEGRATION_TYPE,
                "IntegrationMethod": loadtest.EXPECTED_INTEGRATION_METHOD,
                "ConnectionType": loadtest.EXPECTED_CONNECTION_TYPE,
            }

    class Session:
        @staticmethod
        def create_client(service, **kwargs):
            captured[service] = kwargs
            if service == "sts":
                return StsClient(kwargs["endpoint_url"])
            return ApiClient(kwargs["endpoint_url"])

    import botocore.session

    monkeypatch.setattr(botocore.session, "get_session", lambda: Session())
    attestation = REAL_VALIDATE_LIVE_OWNERSHIP(cfg)

    assert captured["sts"]["endpoint_url"] == loadtest.OFFICIAL_STS_ENDPOINT
    assert captured["apigatewayv2"]["endpoint_url"] == (loadtest.OFFICIAL_APIGATEWAYV2_ENDPOINT)
    for kwargs in captured.values():
        assert kwargs["region_name"] == loadtest.EXPECTED_REGION
        assert kwargs["verify"] is True
        assert kwargs["config"].proxies == {}
        assert kwargs["config"].ignore_configured_endpoint_urls is True
    assert integration_calls == [{"ApiId": "abc123", "IntegrationId": INTEGRATION_ID}]
    assert attestation["route"]["route_key"] == loadtest.EXPECTED_ROUTE_KEY
    assert attestation["route"]["authorization_type"] == "AWS_IAM"
    assert attestation["route"]["target"] == f"integrations/{INTEGRATION_ID}"
    assert attestation["integration"] == {
        "integration_id": INTEGRATION_ID,
        "integration_uri": INTEGRATION_URI,
        "integration_type": "HTTP_PROXY",
        "integration_method": "ANY",
        "connection_type": "VPC_LINK",
    }
    assert attestation["configured_endpoint_urls_ignored"] is True


def test_ownership_guard_requires_exact_account_role_api_and_tags(tmp_path):
    cfg = config(tmp_path)
    caller = {
        "Account": loadtest.EXPECTED_ACCOUNT_ID,
        "Arn": "arn:aws:sts::123456789012:assumed-role/harbormaster-platform/w5-load",
    }
    api = {
        "Name": loadtest.EXPECTED_API_NAME,
        "ProtocolType": "HTTP",
        "ApiEndpoint": "https://abc123.execute-api.us-east-1.amazonaws.com",
        "Tags": {
            "Project": "harbormaster",
            "Environment": "base",
            "ManagedBy": "terraform",
        },
    }
    routes = [
        {
            "RouteId": "route-1",
            "RouteKey": loadtest.EXPECTED_ROUTE_KEY,
            "AuthorizationType": loadtest.EXPECTED_ROUTE_AUTHORIZATION,
            "Target": f"integrations/{INTEGRATION_ID}",
        }
    ]
    integration = {
        "IntegrationId": INTEGRATION_ID,
        "IntegrationUri": INTEGRATION_URI,
        "IntegrationType": loadtest.EXPECTED_INTEGRATION_TYPE,
        "IntegrationMethod": loadtest.EXPECTED_INTEGRATION_METHOD,
        "ConnectionType": loadtest.EXPECTED_CONNECTION_TYPE,
    }
    attestation = loadtest._validate_ownership_records(cfg, caller, api, routes, integration)
    assert attestation["account"] == loadtest.EXPECTED_ACCOUNT_ID
    assert attestation["api_id"] == "abc123"

    with pytest.raises(ValueError, match="account"):
        loadtest._validate_ownership_records(
            cfg, {**caller, "Account": "000000000000"}, api, routes, integration
        )
    with pytest.raises(ValueError, match="caller ARN"):
        loadtest._validate_ownership_records(
            cfg,
            {**caller, "Arn": "arn:aws:iam::123456789012:user/example-admin"},
            api,
            routes,
            integration,
        )
    with pytest.raises(ValueError, match="name"):
        loadtest._validate_ownership_records(
            cfg, caller, {**api, "Name": "other-api"}, routes, integration
        )
    with pytest.raises(ValueError, match="tags"):
        loadtest._validate_ownership_records(cfg, caller, {**api, "Tags": {}}, routes, integration)
    with pytest.raises(ValueError, match="AWS_IAM"):
        loadtest._validate_ownership_records(
            cfg,
            caller,
            api,
            [{**routes[0], "AuthorizationType": "NONE"}],
            integration,
        )
    with pytest.raises(ValueError, match="exactly one"):
        loadtest._validate_ownership_records(cfg, caller, api, routes + routes, integration)
    with pytest.raises(ValueError, match="exactly one"):
        loadtest._validate_ownership_records(
            cfg,
            caller,
            api,
            [{**routes[0], "RouteKey": "GET /v1/score-ais"}],
            integration,
        )

    with pytest.raises(ValueError, match="route target"):
        loadtest._validate_ownership_records(
            cfg,
            caller,
            api,
            [{**routes[0], "Target": "integrations/other1"}],
            integration,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("IntegrationId", "other1"),
        (
            "IntegrationUri",
            "arn:aws:servicediscovery:us-east-1:123456789012:service/srv-other",
        ),
        ("IntegrationType", "AWS_PROXY"),
        ("IntegrationMethod", "POST"),
        ("ConnectionType", "INTERNET"),
    ),
)
def test_ownership_guard_rejects_private_integration_drift(tmp_path, field, value):
    cfg = config(tmp_path)
    caller = {
        "Account": loadtest.EXPECTED_ACCOUNT_ID,
        "Arn": "arn:aws:sts::123456789012:assumed-role/harbormaster-platform/w5-load",
    }
    api = {
        "Name": loadtest.EXPECTED_API_NAME,
        "ProtocolType": "HTTP",
        "ApiEndpoint": "https://abc123.execute-api.us-east-1.amazonaws.com",
        "Tags": {
            "Project": "harbormaster",
            "Environment": "base",
            "ManagedBy": "terraform",
        },
    }
    routes = [
        {
            "RouteId": "route-1",
            "RouteKey": loadtest.EXPECTED_ROUTE_KEY,
            "AuthorizationType": loadtest.EXPECTED_ROUTE_AUTHORIZATION,
            "Target": f"integrations/{INTEGRATION_ID}",
        }
    ]
    integration = {
        "IntegrationId": INTEGRATION_ID,
        "IntegrationUri": INTEGRATION_URI,
        "IntegrationType": loadtest.EXPECTED_INTEGRATION_TYPE,
        "IntegrationMethod": loadtest.EXPECTED_INTEGRATION_METHOD,
        "ConnectionType": loadtest.EXPECTED_CONNECTION_TYPE,
    }

    with pytest.raises(ValueError, match="private integration"):
        loadtest._validate_ownership_records(
            cfg,
            caller,
            api,
            routes,
            {**integration, field: value},
        )


def test_reviewed_source_guard_rejects_dirty_or_mismatched_checkout(tmp_path, monkeypatch):
    cfg = config(tmp_path)
    responses = iter([cfg.expected_git_head, " M scripts/loadtest_signed_serving.py"])
    monkeypatch.setattr(
        loadtest.subprocess,
        "check_output",
        lambda *_args, **_kwargs: next(responses),
    )
    with pytest.raises(ValueError, match="not clean"):
        REAL_VALIDATE_REVIEWED_SOURCE(cfg)

    responses = iter(["f" * 40, ""])
    monkeypatch.setattr(
        loadtest.subprocess,
        "check_output",
        lambda *_args, **_kwargs: next(responses),
    )
    with pytest.raises(ValueError, match="HEAD mismatch"):
        REAL_VALIDATE_REVIEWED_SOURCE(cfg)


@pytest.mark.parametrize(
    ("result", "outcome"),
    [
        (loadtest.RawHttpResult(None, b"", "timeout"), "transport_error"),
        (loadtest.RawHttpResult(503, b"unavailable"), "http_error"),
        (loadtest.RawHttpResult(200, b"not-json"), "contract_error"),
    ],
)
def test_preflight_failure_classification(tmp_path, result, outcome):
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)
    record = loadtest._preflight_record(
        cfg,
        workload,
        transport=lambda *_args: result,
        monotonic=lambda: 0.0,
    )
    assert record["outcome"] == outcome


def test_preflight_sanitizes_transport_exception(tmp_path):
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)

    def transport(*_args):
        raise RuntimeError("secret")

    record = loadtest._preflight_record(cfg, workload, transport=transport, monotonic=lambda: 0.0)
    assert record["error_code"] == "transport_exception"
    assert "secret" not in json.dumps(record)


def test_internal_runner_error_is_sealed_as_failed_evidence(tmp_path):
    clock = FakeClock()
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)
    transport, _calls = unique_success_transport(clock)

    def broken_executor(_workers):
        raise RuntimeError("broken executor")

    summary, exit_code = execute_run(
        cfg,
        workload,
        transport=transport,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.utc_now,
        executor_factory=broken_executor,
    )
    assert exit_code == 1
    assert summary["status"] == "failed"
    assert summary["stop_reason"] == "internal_error"


def test_ledger_terminal_write_failure_gets_separate_failure_marker(tmp_path, monkeypatch):
    clock = FakeClock()
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)
    transport, _calls = unique_success_transport(clock)
    original_write = loadtest.EventLedger.write

    def failing_write(self, record_type, **fields):
        if record_type == "run_ended":
            raise OSError("simulated disk failure")
        return original_write(self, record_type, **fields)

    monkeypatch.setattr(loadtest.EventLedger, "write", failing_write)
    summary, exit_code = execute_run(
        cfg,
        workload,
        transport=transport,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.utc_now,
        executor_factory=ImmediateExecutor,
    )

    assert exit_code == 1
    assert summary["status"] == "failed"
    assert summary["stop_reason"] == "event_ledger_failure"
    failure = json.loads((cfg.artifact_dir / "failure.json").read_text())
    assert failure["valid_for_measurement"] is False
    assert "failure.json" in (cfg.artifact_dir / "evidence-files.sha256").read_text()


def test_git_metadata_has_fail_closed_unknown_shape(monkeypatch):
    monkeypatch.setattr(
        loadtest.subprocess,
        "check_output",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError()),
    )
    assert loadtest._git_metadata() == {"head": None, "dirty": None}


@pytest.mark.parametrize("value", ["bad", "0", "nan", "inf", "-1"])
def test_positive_float_parser_rejects_invalid_values(value):
    with pytest.raises(argparse.ArgumentTypeError):
        loadtest._positive_float(value)


@pytest.mark.parametrize("value", ["bad", "nan", "inf", "-1"])
def test_nonnegative_float_parser_rejects_invalid_values(value):
    with pytest.raises(argparse.ArgumentTypeError):
        loadtest._nonnegative_float(value)


def test_config_from_valid_cli_arguments(tmp_path):
    args = loadtest.build_parser().parse_args(valid_argv(tmp_path))
    cfg = loadtest.config_from_args(args)
    assert cfg.run_id == RUN_ID
    assert cfg.expected_integration_id == INTEGRATION_ID
    assert cfg.expected_integration_uri == INTEGRATION_URI
    assert cfg.planned_request_count == 8


def test_config_accepts_reviewed_nlb_listener_integration_uri(tmp_path):
    nlb_listener_uri = (
        "arn:aws:elasticloadbalancing:us-east-1:123456789012:"
        # pragma: allowlist nextline secret (the listener IDs are placeholders)
        "listener/net/harbormaster-serving/0123456789abcdef/fedcba9876543210"
    )

    cfg = config(tmp_path, expected_integration_uri=nlb_listener_uri)

    assert cfg.expected_integration_uri == nlb_listener_uri


def test_route_key_matches_terraform_proxy_while_request_url_keeps_score_path():
    terraform = Path("infra/terraform/modules/apigw/main.tf").read_text()
    start = terraform.index('resource "aws_apigatewayv2_route" "proxy"')
    next_resource = terraform.find('\nresource "', start + 1)
    proxy_block = terraform[start : next_resource if next_resource >= 0 else None]

    assert loadtest.EXPECTED_ROUTE_KEY == "ANY /{proxy+}"
    assert f'route_key = "{loadtest.EXPECTED_ROUTE_KEY}"' in proxy_block
    assert loadtest.EXPECTED_SCORE_PATH == "/v1/score-ais"
    assert URL.endswith(loadtest.EXPECTED_SCORE_PATH)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("expected_integration_id", "INVALID", "integration ID"),
        (
            "expected_integration_uri",
            "arn:aws:servicediscovery:us-west-2:123456789012:service/srv-wrong",
            "account and region",
        ),
        (
            "expected_integration_uri",
            "arn:aws:s3:us-east-1:123456789012:bucket/wrong",
            "Cloud Map service or load balancer listener",
        ),
    ),
)
def test_config_rejects_unreviewable_integration_binding(tmp_path, field, value, message):
    with pytest.raises(ValueError, match=message):
        config(tmp_path, **{field: value})


def test_inherited_pipe_grant_is_required_consumed_once_and_claim_bound(tmp_path, monkeypatch):
    cfg = config(tmp_path)
    claim = loadtest._claim_supervisor_evidence(cfg)
    grant = replace(loadtest._build_supervisor_grant(cfg, claim), parent_pid=os.getppid())
    read_fd, write_fd = os.pipe()
    os.write(
        write_fd,
        json.dumps(loadtest.asdict(grant), separators=(",", ":"), sort_keys=True).encode() + b"\n",
    )
    environment = {"HM_W5_SUPERVISOR_FD": str(read_fd)}
    consumed, reader = loadtest._consume_supervisor_grant(cfg, environment=environment)
    try:
        assert consumed == grant
        assert environment == {}
        assert loadtest._ACTIVE_SUPERVISOR_GRANT is consumed
    finally:
        monkeypatch.setattr(loadtest, "_ACTIVE_SUPERVISOR_GRANT", None)
        os.close(write_fd)
        reader.close()

    with pytest.raises(ValueError, match="missing its inherited capability"):
        loadtest._consume_supervisor_grant(cfg, environment={})


def test_inherited_pipe_grant_rejects_closed_fd_and_claim_tamper(tmp_path):
    cfg = config(tmp_path)
    read_fd, write_fd = os.pipe()
    os.close(read_fd)
    os.close(write_fd)
    with pytest.raises(ValueError, match="not open"):
        loadtest._consume_supervisor_grant(
            cfg,
            environment={"HM_W5_SUPERVISOR_FD": str(read_fd)},
        )

    claim = loadtest._claim_supervisor_evidence(cfg)
    grant = replace(loadtest._build_supervisor_grant(cfg, claim), parent_pid=os.getppid())
    claim.claim_path.write_text("{}\n")
    read_fd, write_fd = os.pipe()
    os.write(write_fd, json.dumps(loadtest.asdict(grant)).encode() + b"\n")
    with pytest.raises(ValueError, match="claim checksum"):
        loadtest._consume_supervisor_grant(
            cfg,
            environment={"HM_W5_SUPERVISOR_FD": str(read_fd)},
        )
    os.close(write_fd)


def test_claim_target_tamper_fails_after_attacker_rehashes_files(tmp_path):
    cfg = config(tmp_path)
    claim = loadtest._claim_supervisor_evidence(cfg)
    claim_payload = json.loads(claim.claim_path.read_text())
    claim_payload["target_binding"]["integration_id"] = "other1"
    claim.claim_path.write_text(json.dumps(claim_payload, indent=2, sort_keys=True) + "\n")
    tampered_sha256 = loadtest._sha256(claim.claim_path)
    claim.claim_manifest_path.write_text(f"{tampered_sha256}  {claim.claim_path.name}\n")
    grant = replace(
        loadtest._build_supervisor_grant(cfg, claim),
        parent_pid=os.getppid(),
        claim_sha256=tampered_sha256,
    )
    read_fd, write_fd = os.pipe()
    os.write(write_fd, json.dumps(loadtest.asdict(grant)).encode() + b"\n")
    try:
        with pytest.raises(ValueError, match="claim binding"):
            loadtest._consume_supervisor_grant(
                cfg,
                environment={"HM_W5_SUPERVISOR_FD": str(read_fd)},
            )
    finally:
        os.close(write_fd)


def test_parent_liveness_pipe_eof_requests_child_stop(monkeypatch):
    read_fd, write_fd = os.pipe()
    reader = os.fdopen(read_fd, "rb", buffering=0)
    os.close(write_fd)
    stopped = threading.Event()
    signals = []
    monkeypatch.setattr(loadtest.os, "kill", lambda pid, sig: signals.append((pid, sig)))
    loadtest._watch_parent_liveness(reader, stopped)
    reader.close()
    assert stopped.is_set()
    assert signals == [(os.getpid(), signal.SIGTERM)]


def test_supervisor_signal_state_discards_pending_before_restore():
    check = """
import os
import signal

from scripts import loadtest_signed_serving as loadtest

initial_handlers = {
    signum: signal.getsignal(signum) for signum in loadtest._supervisor_signals()
}
seen = []

def prior_handler(signum, _frame):
    seen.append(signum)

for signum in loadtest._supervisor_signals():
    signal.signal(signum, prior_handler)
previous_handlers = {
    signum: signal.getsignal(signum) for signum in loadtest._supervisor_signals()
}
prior_mask = loadtest._block_supervisor_signals()
try:
    interrupt_state = loadtest._SupervisorInterruptState()
    for signum in loadtest._supervisor_signals():
        signal.signal(signum, interrupt_state.handle)
    os.kill(os.getpid(), signal.SIGTERM)
    os.kill(os.getpid(), signal.SIGHUP)
    pending = loadtest._pending_supervisor_signals()
    assert signal.SIGTERM in pending
    assert signal.SIGHUP in pending
    loadtest._restore_supervisor_signal_state(previous_handlers, prior_mask)
    assert loadtest._pending_supervisor_signals() == set()
    assert signal.pthread_sigmask(signal.SIG_BLOCK, set()) == prior_mask
    for signum, previous in previous_handlers.items():
        assert signal.getsignal(signum) == previous
    assert seen == []
    os.kill(os.getpid(), signal.SIGTERM)
    assert seen == [signal.SIGTERM]
finally:
    loadtest._block_supervisor_signals()
    for signum in loadtest._supervisor_signals():
        signal.signal(signum, signal.SIG_IGN)
    loadtest._restore_signal_mask(prior_mask)
    for signum, initial in initial_handlers.items():
        signal.signal(signum, initial)
"""
    completed = subprocess.run(
        [sys.executable, "-c", check],
        cwd=loadtest.REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert completed.returncode == 0, completed.stderr


def test_stale_pending_observation_never_calls_blocking_sigwait(monkeypatch, tmp_path):
    artifact_dir = tmp_path / "stale-pending-run"
    cfg = config(tmp_path, artifact_dir=artifact_dir)
    claim = loadtest._claim_supervisor_evidence(cfg)
    grant = loadtest._build_supervisor_grant(cfg, claim)
    now = time.monotonic()
    plan = loadtest.SupervisorPlan(
        cutoff_utc=datetime.now(UTC) + timedelta(seconds=3),
        started_utc=datetime.now(UTC),
        started_monotonic=now,
        graceful_stop_monotonic=now + 2.0,
        hard_stop_monotonic=now + 3.0,
        required_window_seconds=1.0,
        termination_grace_seconds=1.0,
    )

    monkeypatch.setattr(
        loadtest,
        "_pending_supervisor_signals",
        lambda: {signal.SIGTERM},
    )
    monkeypatch.setattr(
        loadtest.signal,
        "sigwait",
        lambda *_args: (_ for _ in ()).throw(AssertionError("blocking sigwait is forbidden")),
    )

    exit_code = loadtest._supervise_child(
        ["must-not-launch"],
        environment=os.environ.copy(),
        artifact_dir=artifact_dir,
        grant=grant,
        plan=plan,
        evidence_validator=lambda: True,
        popen_factory=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("stale pending observation must stop before launch")
        ),
    )

    assert exit_code == 130
    stop = json.loads(
        artifact_dir.with_name(f"{artifact_dir.name}.supervisor-stop.json").read_text()
    )
    assert stop["reason"] == "child_start_interrupted"


def test_supervisor_claim_is_exclusive_and_consumes_the_artifact_namespace(tmp_path):
    cfg = config(tmp_path)
    claim = loadtest._claim_supervisor_evidence(cfg)
    assert claim.claim_path.is_file()
    assert stat.S_IMODE(claim.claim_path.stat().st_mode) == 0o600
    with pytest.raises(FileExistsError, match="already exists"):
        loadtest._claim_supervisor_evidence(cfg)


def test_supervised_run_spec_and_manifest_bind_the_immutable_claim(tmp_path, monkeypatch):
    clock = FakeClock()
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)
    claim = loadtest._claim_supervisor_evidence(cfg)
    grant = replace(loadtest._build_supervisor_grant(cfg, claim), parent_pid=os.getppid())
    monkeypatch.setattr(loadtest, "_ACTIVE_SUPERVISOR_GRANT", grant)
    transport, _calls = unique_success_transport(clock)
    summary, exit_code = loadtest.execute_run(
        cfg,
        workload,
        live_confirmation=loadtest.CONFIRM_PHRASE,
        transport=transport,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.utc_now,
        executor_factory=ImmediateExecutor,
        supervisor_grant=grant,
    )
    assert exit_code == 0
    assert summary["valid_for_measurement"] is True
    run_spec = json.loads((cfg.artifact_dir / "run-spec.json").read_text())
    claim_payload = json.loads(claim.claim_path.read_text())
    expected_binding = loadtest._expected_target_binding(cfg)
    assert claim_payload["target_binding"] == expected_binding
    assert grant.expected_integration_id == INTEGRATION_ID
    assert grant.expected_integration_uri == INTEGRATION_URI
    assert run_spec["evidence_binding"]["claim_id"] == claim.claim_id
    assert run_spec["evidence_binding"]["claim_sha256"] == claim.claim_sha256
    assert run_spec["target"]["reviewed_binding"] == expected_binding
    assert run_spec["target"]["ownership"]["integration"] == {
        "integration_id": INTEGRATION_ID,
        "integration_uri": INTEGRATION_URI,
        "integration_type": "HTTP_PROXY",
        "integration_method": "ANY",
        "connection_type": "VPC_LINK",
    }
    assert run_spec["config"]["expected_integration_id"] == INTEGRATION_ID
    assert run_spec["config"]["expected_integration_uri"] == INTEGRATION_URI
    assert loadtest._verify_evidence_binding(cfg, grant) is True
    assert loadtest._verify_supervisor_complete(cfg.artifact_dir, grant) is False
    now = datetime.now(UTC)
    loadtest._write_supervisor_complete(
        cfg.artifact_dir,
        grant=grant,
        child_exit_code=0,
        started_at=now,
        completed_at=now,
    )
    assert loadtest._verify_supervisor_complete(cfg.artifact_dir, grant) is True


def test_supervisor_grant_rejects_integration_binding_tamper(tmp_path):
    cfg = config(tmp_path)
    claim = loadtest._claim_supervisor_evidence(cfg)
    grant = replace(loadtest._build_supervisor_grant(cfg, claim), parent_pid=os.getppid())

    with pytest.raises(ValueError, match="reviewed run configuration"):
        loadtest._validate_supervisor_grant(
            cfg,
            replace(grant, expected_integration_id="other1"),
        )
    with pytest.raises(ValueError, match="reviewed run configuration"):
        loadtest._validate_supervisor_grant(
            cfg,
            replace(
                grant,
                expected_integration_uri=(
                    "arn:aws:servicediscovery:us-east-1:123456789012:service/srv-other"
                ),
            ),
        )


def test_semantically_tampered_target_evidence_fails_even_with_fresh_file_hash(
    tmp_path, monkeypatch
):
    clock = FakeClock()
    cfg = config(tmp_path)
    workload = loadtest.load_workload(FIXTURE)
    claim = loadtest._claim_supervisor_evidence(cfg)
    grant = replace(loadtest._build_supervisor_grant(cfg, claim), parent_pid=os.getppid())
    monkeypatch.setattr(loadtest, "_ACTIVE_SUPERVISOR_GRANT", grant)
    transport, _calls = unique_success_transport(clock)
    summary, exit_code = loadtest.execute_run(
        cfg,
        workload,
        live_confirmation=loadtest.CONFIRM_PHRASE,
        transport=transport,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.utc_now,
        executor_factory=ImmediateExecutor,
        supervisor_grant=grant,
    )
    assert exit_code == 0
    assert summary["valid_for_measurement"] is True
    assert loadtest._verify_evidence_binding(cfg, grant) is True

    run_spec_path = cfg.artifact_dir / "run-spec.json"
    run_spec = json.loads(run_spec_path.read_text())
    run_spec["target"]["reviewed_binding"]["integration_uri"] = (
        "arn:aws:servicediscovery:us-east-1:123456789012:service/srv-tampered"
    )
    run_spec_path.write_text(json.dumps(run_spec, indent=2, sort_keys=True) + "\n")
    manifest_path = cfg.artifact_dir / "evidence-files.sha256"
    lines = manifest_path.read_text().splitlines()
    lines = [
        f"{loadtest._sha256(run_spec_path)}  run-spec.json"
        if line.endswith("  run-spec.json")
        else line
        for line in lines
    ]
    manifest_path.write_text("\n".join(lines) + "\n")

    assert loadtest._verify_evidence_binding(cfg, grant) is False


def test_supervisor_complete_and_stop_verdicts_are_mutually_exclusive(tmp_path):
    artifact_dir = tmp_path / "terminal-verdict-run"
    cfg = config(tmp_path, artifact_dir=artifact_dir)
    claim = loadtest._claim_supervisor_evidence(cfg)
    grant = loadtest._build_supervisor_grant(cfg, claim)
    artifact_dir.mkdir(mode=0o700)
    (artifact_dir / "summary.json").write_text(
        json.dumps(
            {
                "run_id": RUN_ID,
                "status": "completed",
                "valid_for_measurement": True,
                "targets_met": True,
                "verdict": "passed",
            }
        )
    )
    (artifact_dir / "evidence-files.sha256").write_text("reviewed manifest\n")
    now = datetime.now(UTC)
    complete_path = loadtest._write_supervisor_complete(
        artifact_dir,
        grant=grant,
        child_exit_code=0,
        started_at=now,
        completed_at=now,
    )
    assert complete_path.is_file()
    assert json.loads(complete_path.read_text())["target_binding"] == (
        loadtest._expected_target_binding(grant)
    )
    with pytest.raises(ValueError, match="complete verdict"):
        loadtest._write_supervisor_stop(
            artifact_dir,
            grant=grant,
            reason="must_not_coexist",
            child_exit_code=0,
            terminate_sent=False,
            kill_sent=False,
            started_at=now,
            stopped_at=now,
        )

    stop_artifact = tmp_path / "stop-first-run"
    stop_cfg = config(tmp_path, artifact_dir=stop_artifact)
    stop_claim = loadtest._claim_supervisor_evidence(stop_cfg)
    stop_grant = loadtest._build_supervisor_grant(stop_cfg, stop_claim)
    stop_path = loadtest._write_supervisor_stop(
        stop_artifact,
        grant=stop_grant,
        reason="test_stop",
        child_exit_code=1,
        terminate_sent=False,
        kill_sent=False,
        started_at=now,
        stopped_at=now,
    )
    assert json.loads(stop_path.read_text())["target_binding"] == (
        loadtest._expected_target_binding(stop_grant)
    )
    stop_artifact.mkdir(mode=0o700)
    (stop_artifact / "summary.json").write_text("{}\n")
    (stop_artifact / "evidence-files.sha256").write_text("reviewed manifest\n")
    with pytest.raises(ValueError, match="stop verdict"):
        loadtest._write_supervisor_complete(
            stop_artifact,
            grant=stop_grant,
            child_exit_code=1,
            started_at=now,
            completed_at=now,
        )


def test_supervisor_returns_child_status_without_stop_artifact(tmp_path):
    artifact_dir = tmp_path / "normal-run"
    cfg = config(tmp_path, artifact_dir=artifact_dir)
    claim = loadtest._claim_supervisor_evidence(cfg)
    grant = loadtest._build_supervisor_grant(cfg, claim)
    artifact_dir.mkdir(mode=0o700)
    (artifact_dir / "summary.json").write_text(
        json.dumps(
            {
                "run_id": RUN_ID,
                "status": "failed",
                "valid_for_measurement": False,
                "targets_met": False,
                "verdict": "invalid_measurement",
            }
        )
    )
    (artifact_dir / "evidence-files.sha256").write_text("reviewed manifest\n")
    now = time.monotonic()
    plan = loadtest.SupervisorPlan(
        cutoff_utc=datetime.now(UTC) + timedelta(seconds=3),
        started_utc=datetime.now(UTC),
        started_monotonic=now,
        graceful_stop_monotonic=now + 2.0,
        hard_stop_monotonic=now + 3.0,
        required_window_seconds=1.0,
        termination_grace_seconds=1.0,
    )
    exit_code = loadtest._supervise_child(
        [
            sys.executable,
            "-c",
            (
                "import os; os.read(int(os.environ['HM_W5_SUPERVISOR_FD']), 4096); "
                "raise SystemExit(7)"
            ),
        ],
        environment=os.environ.copy(),
        artifact_dir=artifact_dir,
        grant=grant,
        plan=plan,
        evidence_validator=lambda: True,
    )
    assert exit_code == 7
    assert not artifact_dir.with_name(f"{artifact_dir.name}.supervisor-stop.json").exists()
    complete_path = artifact_dir.with_name(f"{artifact_dir.name}.supervisor-complete.json")
    complete = json.loads(complete_path.read_text())
    assert complete["supervisor_accepted_evidence"] is True
    assert complete["child_exit_code"] == 7
    assert complete["claim_id"] == claim.claim_id


def test_supervisor_parent_interrupt_terminates_reaps_and_writes_invalid_verdict(tmp_path):
    artifact_dir = tmp_path / "interrupted-run"
    cfg = config(tmp_path, artifact_dir=artifact_dir)
    claim = loadtest._claim_supervisor_evidence(cfg)
    grant = loadtest._build_supervisor_grant(cfg, claim)
    now = time.monotonic()
    plan = loadtest.SupervisorPlan(
        cutoff_utc=datetime.now(UTC) + timedelta(seconds=3),
        started_utc=datetime.now(UTC),
        started_monotonic=now,
        graceful_stop_monotonic=now + 2.0,
        hard_stop_monotonic=now + 3.0,
        required_window_seconds=1.0,
        termination_grace_seconds=1.0,
    )

    class InterruptedChild:
        def __init__(self):
            self.returncode = None
            self.wait_calls = 0
            self.terminated = False
            self.killed = False

        def wait(self, timeout=None):
            del timeout
            self.wait_calls += 1
            if self.wait_calls == 1:
                raise KeyboardInterrupt
            self.returncode = -15
            return self.returncode

        def poll(self):
            return self.returncode

        def terminate(self):
            self.terminated = True

        def kill(self):
            self.killed = True

    child = InterruptedChild()

    def interrupted_child_factory(*_args, **kwargs):
        child.capability_fd = os.dup(kwargs["pass_fds"][0])
        return child

    exit_code = loadtest._supervise_child(
        ["ignored"],
        environment=os.environ.copy(),
        artifact_dir=artifact_dir,
        grant=grant,
        plan=plan,
        evidence_validator=lambda: True,
        popen_factory=interrupted_child_factory,
    )
    os.close(child.capability_fd)

    assert exit_code == 130
    assert child.terminated is True
    assert child.killed is False
    assert child.returncode == -15
    stop = json.loads(
        artifact_dir.with_name(f"{artifact_dir.name}.supervisor-stop.json").read_text()
    )
    assert stop["reason"] == "supervisor_interrupted"
    assert stop["valid_for_measurement"] is False


def test_second_interrupt_during_cleanup_still_kills_reaps_and_writes_stop(tmp_path):
    artifact_dir = tmp_path / "repeat-interrupt-run"
    cfg = config(tmp_path, artifact_dir=artifact_dir)
    claim = loadtest._claim_supervisor_evidence(cfg)
    grant = loadtest._build_supervisor_grant(cfg, claim)
    now = time.monotonic()
    plan = loadtest.SupervisorPlan(
        cutoff_utc=datetime.now(UTC) + timedelta(seconds=3),
        started_utc=datetime.now(UTC),
        started_monotonic=now,
        graceful_stop_monotonic=now + 2.0,
        hard_stop_monotonic=now + 3.0,
        required_window_seconds=1.0,
        termination_grace_seconds=1.0,
    )

    class RepeatedInterruptChild:
        def __init__(self):
            self.returncode = None
            self.wait_calls = 0
            self.terminated = False
            self.killed = False

        def wait(self, timeout=None):
            del timeout
            self.wait_calls += 1
            if self.wait_calls == 1:
                raise loadtest._SupervisorInterrupted
            if self.wait_calls == 2:
                raise KeyboardInterrupt
            if self.wait_calls == 3:
                raise loadtest.subprocess.TimeoutExpired("child", 0)
            self.returncode = -9
            return self.returncode

        def poll(self):
            return self.returncode

        def terminate(self):
            self.terminated = True

        def kill(self):
            self.killed = True

    child = RepeatedInterruptChild()

    def repeated_interrupt_factory(*_args, **kwargs):
        child.capability_fd = os.dup(kwargs["pass_fds"][0])
        return child

    exit_code = loadtest._supervise_child(
        ["ignored"],
        environment=os.environ.copy(),
        artifact_dir=artifact_dir,
        grant=grant,
        plan=plan,
        evidence_validator=lambda: True,
        popen_factory=repeated_interrupt_factory,
    )
    os.close(child.capability_fd)

    assert exit_code == 130
    assert child.terminated is True
    assert child.killed is True
    assert child.returncode == -9
    stop = json.loads(
        artifact_dir.with_name(f"{artifact_dir.name}.supervisor-stop.json").read_text()
    )
    assert stop["reason"] == "supervisor_interrupted"
    assert stop["kill_sent"] is True


def test_interrupt_during_process_launch_terminates_claim_with_stop_verdict(tmp_path):
    artifact_dir = tmp_path / "launch-interrupt-run"
    cfg = config(tmp_path, artifact_dir=artifact_dir)
    claim = loadtest._claim_supervisor_evidence(cfg)
    grant = loadtest._build_supervisor_grant(cfg, claim)
    now = time.monotonic()
    plan = loadtest.SupervisorPlan(
        cutoff_utc=datetime.now(UTC) + timedelta(seconds=3),
        started_utc=datetime.now(UTC),
        started_monotonic=now,
        graceful_stop_monotonic=now + 2.0,
        hard_stop_monotonic=now + 3.0,
        required_window_seconds=1.0,
        termination_grace_seconds=1.0,
    )

    def interrupted_launch(*_args, **_kwargs):
        raise loadtest._SupervisorInterrupted

    exit_code = loadtest._supervise_child(
        ["ignored"],
        environment=os.environ.copy(),
        artifact_dir=artifact_dir,
        grant=grant,
        plan=plan,
        evidence_validator=lambda: True,
        popen_factory=interrupted_launch,
    )
    assert exit_code == 130
    stop = json.loads(
        artifact_dir.with_name(f"{artifact_dir.name}.supervisor-stop.json").read_text()
    )
    assert stop["reason"] == "child_start_interrupted"
    assert stop["claim_id"] == claim.claim_id


def test_signal_after_pipe_close_before_sentinel_does_not_double_close(monkeypatch, tmp_path):
    artifact_dir = tmp_path / "close-signal-run"
    cfg = config(tmp_path, artifact_dir=artifact_dir)
    claim = loadtest._claim_supervisor_evidence(cfg)
    grant = loadtest._build_supervisor_grant(cfg, claim)
    now = time.monotonic()
    plan = loadtest.SupervisorPlan(
        cutoff_utc=datetime.now(UTC) + timedelta(seconds=3),
        started_utc=datetime.now(UTC),
        started_monotonic=now,
        graceful_stop_monotonic=now + 2.0,
        hard_stop_monotonic=now + 3.0,
        required_window_seconds=1.0,
        termination_grace_seconds=1.0,
    )
    closed_fds = []
    interrupt_state = loadtest._SupervisorInterruptState()

    class CloseSignalChild:
        def __init__(self):
            self.returncode = None
            self.capability_fd = -1

        def wait(self, timeout=None):
            del timeout
            self.returncode = -15
            return self.returncode

        def poll(self):
            return self.returncode

        def terminate(self):
            self.returncode = -15

        def kill(self):
            raise AssertionError("close-signal cleanup should not need SIGKILL")

    child = CloseSignalChild()
    real_close = loadtest._close_supervisor_fd

    def close_fd(fd):
        real_close(fd)
        closed_fds.append(fd)
        if len(closed_fds) == 1:
            interrupt_state.handle(signal.SIGTERM, None)

    def child_factory(*_args, **kwargs):
        child.capability_fd = os.dup(kwargs["pass_fds"][0])
        return child

    monkeypatch.setattr(loadtest, "_SupervisorInterruptState", lambda: interrupt_state)
    monkeypatch.setattr(loadtest, "_close_supervisor_fd", close_fd)

    try:
        exit_code = loadtest._supervise_child(
            ["ignored"],
            environment=os.environ.copy(),
            artifact_dir=artifact_dir,
            grant=grant,
            plan=plan,
            evidence_validator=lambda: True,
            popen_factory=child_factory,
        )
    finally:
        if child.capability_fd >= 0:
            os.close(child.capability_fd)

    assert exit_code == 130
    assert len(closed_fds) == 2
    assert len(closed_fds) == len(set(closed_fds))
    stop = json.loads(
        artifact_dir.with_name(f"{artifact_dir.name}.supervisor-stop.json").read_text()
    )
    assert stop["reason"] == "supervisor_interrupted"


def test_signal_after_real_child_creation_before_launch_return_is_reaped(tmp_path):
    artifact_dir = tmp_path / "post-fork-signal-run"
    cfg = config(tmp_path, artifact_dir=artifact_dir)
    claim = loadtest._claim_supervisor_evidence(cfg)
    grant = loadtest._build_supervisor_grant(cfg, claim)
    now = time.monotonic()
    plan = loadtest.SupervisorPlan(
        cutoff_utc=datetime.now(UTC) + timedelta(seconds=3),
        started_utc=datetime.now(UTC),
        started_monotonic=now,
        graceful_stop_monotonic=now + 2.0,
        hard_stop_monotonic=now + 3.0,
        required_window_seconds=1.0,
        termination_grace_seconds=1.0,
    )
    launched = []

    def launch_then_signal(command, **kwargs):
        child = subprocess.Popen(command, **kwargs)
        launched.append(child)
        os.kill(os.getpid(), signal.SIGTERM)
        return child

    command = [
        sys.executable,
        "-c",
        (
            "import os,signal,time; "
            "signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGTERM}); "
            "os.read(int(os.environ['HM_W5_SUPERVISOR_FD']), 4096); "
            "time.sleep(60)"
        ),
    ]
    exit_code = loadtest._supervise_child(
        command,
        environment=os.environ.copy(),
        artifact_dir=artifact_dir,
        grant=grant,
        plan=plan,
        evidence_validator=lambda: True,
        popen_factory=launch_then_signal,
    )

    assert exit_code == 130
    assert len(launched) == 1
    assert launched[0].poll() is not None
    stop = json.loads(
        artifact_dir.with_name(f"{artifact_dir.name}.supervisor-stop.json").read_text()
    )
    assert stop["reason"] == "supervisor_interrupted"
    assert stop["terminate_sent"] is True
    assert stop["kill_sent"] is False


def test_signal_during_evidence_validation_prevents_complete_verdict(monkeypatch, tmp_path):
    artifact_dir = tmp_path / "evidence-signal-run"
    cfg = config(tmp_path, artifact_dir=artifact_dir)
    claim = loadtest._claim_supervisor_evidence(cfg)
    grant = loadtest._build_supervisor_grant(cfg, claim)
    now = time.monotonic()
    plan = loadtest.SupervisorPlan(
        cutoff_utc=datetime.now(UTC) + timedelta(seconds=3),
        started_utc=datetime.now(UTC),
        started_monotonic=now,
        graceful_stop_monotonic=now + 2.0,
        hard_stop_monotonic=now + 3.0,
        required_window_seconds=1.0,
        termination_grace_seconds=1.0,
    )
    interrupt_state = loadtest._SupervisorInterruptState()
    monkeypatch.setattr(loadtest, "_SupervisorInterruptState", lambda: interrupt_state)

    def interrupting_validator():
        interrupt_state.handle(signal.SIGTERM, None)
        return True

    exit_code = loadtest._supervise_child(
        [
            sys.executable,
            "-c",
            "import os; os.read(int(os.environ['HM_W5_SUPERVISOR_FD']), 4096)",
        ],
        environment=os.environ.copy(),
        artifact_dir=artifact_dir,
        grant=grant,
        plan=plan,
        evidence_validator=interrupting_validator,
    )

    assert exit_code == 130
    stop_path = artifact_dir.with_name(f"{artifact_dir.name}.supervisor-stop.json")
    complete_path = artifact_dir.with_name(f"{artifact_dir.name}.supervisor-complete.json")
    assert json.loads(stop_path.read_text())["reason"] == "supervisor_interrupted"
    assert not complete_path.exists()


def test_normal_child_with_unbound_evidence_gets_controlling_invalid_verdict(tmp_path):
    artifact_dir = tmp_path / "binding-failure-run"
    cfg = config(tmp_path, artifact_dir=artifact_dir)
    claim = loadtest._claim_supervisor_evidence(cfg)
    grant = loadtest._build_supervisor_grant(cfg, claim)
    now = time.monotonic()
    plan = loadtest.SupervisorPlan(
        cutoff_utc=datetime.now(UTC) + timedelta(seconds=3),
        started_utc=datetime.now(UTC),
        started_monotonic=now,
        graceful_stop_monotonic=now + 2.0,
        hard_stop_monotonic=now + 3.0,
        required_window_seconds=1.0,
        termination_grace_seconds=1.0,
    )
    exit_code = loadtest._supervise_child(
        [
            sys.executable,
            "-c",
            (
                "import os; os.read(int(os.environ['HM_W5_SUPERVISOR_FD']), 4096); "
                "raise SystemExit(0)"
            ),
        ],
        environment=os.environ.copy(),
        artifact_dir=artifact_dir,
        grant=grant,
        plan=plan,
        evidence_validator=lambda: False,
    )
    assert exit_code == 124
    stop = json.loads(
        artifact_dir.with_name(f"{artifact_dir.name}.supervisor-stop.json").read_text()
    )
    assert stop["reason"] == "evidence_binding_failure"
    assert stop["claim_id"] == claim.claim_id
    assert stop["artifact_dir"] == str(artifact_dir.resolve())


def test_supervisor_kills_a_child_that_ignores_sigterm_and_writes_invalid_verdict(tmp_path):
    artifact_dir = tmp_path / "hung-run"
    cfg = config(tmp_path, artifact_dir=artifact_dir)
    claim = loadtest._claim_supervisor_evidence(cfg)
    grant = loadtest._build_supervisor_grant(cfg, claim)
    now = time.monotonic()
    plan = loadtest.SupervisorPlan(
        cutoff_utc=datetime.now(UTC) + timedelta(seconds=0.5),
        started_utc=datetime.now(UTC),
        started_monotonic=now,
        graceful_stop_monotonic=now + 0.3,
        hard_stop_monotonic=now + 0.5,
        required_window_seconds=0.1,
        termination_grace_seconds=0.2,
    )
    command = [
        sys.executable,
        "-c",
        ("import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)"),
    ]
    exit_code = loadtest._supervise_child(
        command,
        environment=os.environ.copy(),
        artifact_dir=artifact_dir,
        grant=grant,
        plan=plan,
        evidence_validator=lambda: True,
    )

    assert exit_code == 124
    stop_path = artifact_dir.with_name(f"{artifact_dir.name}.supervisor-stop.json")
    payload = json.loads(stop_path.read_text())
    assert payload["valid_for_measurement"] is False
    assert payload["reason"] == "hard_deadline_kill"
    assert payload["terminate_sent"] is True
    assert payload["kill_sent"] is True
    assert not (artifact_dir / "summary.json").exists()
    manifest_path = stop_path.with_suffix(".sha256")
    manifest_lines = manifest_path.read_text().splitlines()
    assert len(manifest_lines) == 2
    digest, name = manifest_lines[0].split("  ", 1)
    assert name == stop_path.name
    assert hashlib.sha256(stop_path.read_bytes()).hexdigest() == digest
    claim_digest, claim_name = manifest_lines[1].split("  ", 1)
    assert claim_name == claim.claim_path.name
    assert claim_digest == hashlib.sha256(claim.claim_path.read_bytes()).hexdigest()


def test_main_success_path_is_local_when_execution_is_injected(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("HM_W5_SUPERVISOR_FD", raising=False)
    monkeypatch.setenv("HM_W5_SUPERVISOR_PARENT_PID", str(os.getppid()))
    monkeypatch.setattr(loadtest, "supervise_cli", lambda *_args, **_kwargs: 0)
    assert loadtest.main(valid_argv(tmp_path)) == 0
    assert capsys.readouterr().out == ""


def test_supervise_cli_forces_configured_endpoint_urls_to_be_ignored(tmp_path, monkeypatch):
    cfg = config(tmp_path)
    captured = {}
    plan = object()
    monkeypatch.setattr(loadtest, "build_supervisor_plan", lambda *_args, **_kwargs: plan)

    def capture_child(_command, **kwargs):
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(loadtest, "_supervise_child", capture_child)
    assert loadtest.supervise_cli(valid_argv(tmp_path), cfg) == 0
    assert captured["environment"]["AWS_IGNORE_CONFIGURED_ENDPOINT_URLS"] == "true"
    assert captured["plan"] is plan
    assert captured["grant"].run_id == RUN_ID


def test_signal_after_claim_before_launch_gets_terminal_stop_verdict(tmp_path, monkeypatch):
    isolation_flag = "HM_W5_TEST_SIGNAL_AFTER_CLAIM_CHILD"
    if os.environ.get(isolation_flag) != "1":
        environment = os.environ.copy()
        environment[isolation_flag] = "1"
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                f"{Path(__file__).resolve()}::{test_signal_after_claim_before_launch_gets_terminal_stop_verdict.__name__}",
            ],
            cwd=loadtest.REPO_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
            timeout=20,
        )
        assert completed.returncode == 0, completed.stdout + completed.stderr
        return

    started = datetime.now(UTC)
    base = config(tmp_path)
    cfg = replace(
        base,
        operator_cutoff_utc=(
            started + timedelta(seconds=base.required_supervised_window_seconds + 5)
        ).isoformat(),
    )
    real_claim = loadtest._claim_supervisor_evidence

    def claim_then_signal(run_config):
        claim = real_claim(run_config)
        os.kill(os.getpid(), signal.SIGTERM)
        return claim

    monkeypatch.setattr(loadtest, "_claim_supervisor_evidence", claim_then_signal)
    assert loadtest.supervise_cli(valid_argv(tmp_path), cfg) == 130
    stop_path = cfg.artifact_dir.with_name(f"{cfg.artifact_dir.name}.supervisor-stop.json")
    stop = json.loads(stop_path.read_text())
    assert stop["reason"] == "child_start_interrupted"
    assert not cfg.artifact_dir.with_name(
        f"{cfg.artifact_dir.name}.supervisor-complete.json"
    ).exists()


@pytest.mark.parametrize("error", [FileExistsError(), ValueError("bad parent")])
def test_main_maps_operator_execution_errors_to_exit_two(tmp_path, monkeypatch, error):
    monkeypatch.delenv("HM_W5_SUPERVISOR_FD", raising=False)
    monkeypatch.setattr(
        loadtest,
        "supervise_cli",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(error),
    )
    with pytest.raises(SystemExit) as raised:
        loadtest.main(valid_argv(tmp_path))
    assert raised.value.code == 2
