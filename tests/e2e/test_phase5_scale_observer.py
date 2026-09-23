"""Pure timeline tests for the read-only W4 scale observer."""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from scripts.observe_phase5_scale import (
    INFERENCE_TIMEOUT_SECONDS,
    bounded_inference_status,
    capture_baseline,
    deployment_replicas,
    observe_scale,
    signed_inference_status,
    validate_api_gateway_url,
    validate_positive_finite,
    wait_for_load_start,
)

RUN_ID = "12345678-1234-4234-9234-123456789abc"
OTHER_RUN_ID = "87654321-4321-4321-8321-cba987654321"
LOAD_ARTIFACT = Path("/tmp/harbormaster-w4-load.json").resolve()
OBSERVER_ARTIFACT = Path("/tmp/harbormaster-w4-scale.json").resolve()


class FakeClock:
    def __init__(self):
        self.seconds = 0.0
        self.base = datetime(2026, 7, 13, tzinfo=UTC)

    def monotonic(self):
        return self.seconds

    def sleep(self, seconds):
        self.seconds += seconds

    def now(self):
        return self.base + timedelta(seconds=self.seconds)


def ready_baseline(clock: FakeClock) -> dict:
    return {
        "status": "ready_for_load",
        "captured_at": clock.base.isoformat().replace("+00:00", "Z"),
        "desired_replicas": 0,
        "available_replicas": 0,
        "inference_http_status": 503,
        "inference_error": "no healthy target",
    }


def bound_load_payload(clock: FakeClock, status: str = "completed") -> dict:
    payload = {
        "schema_version": 1,
        "status": status,
        "run_id": RUN_ID,
        "load_artifact": str(LOAD_ARTIFACT),
        "observer_ready_path": str(OBSERVER_ARTIFACT),
        "started_at": clock.base.isoformat().replace("+00:00", "Z"),
    }
    if status in {"completed", "failed"}:
        payload["ended_at"] = clock.base.isoformat().replace("+00:00", "Z")
    if status == "failed":
        payload["error"] = "RuntimeError: injected load failure"
    return payload


def load_guard(clock: FakeClock, payload: dict | None = None) -> dict:
    load_payload = payload if payload is not None else bound_load_payload(clock)
    return {
        "read_load_artifact": lambda: load_payload,
        "expected_run_id": RUN_ID,
        "expected_load_artifact": LOAD_ARTIFACT,
        "expected_observer_path": OBSERVER_ARTIFACT,
    }


def test_baseline_proves_zero_and_non_serving_route_before_load():
    clock = FakeClock()
    baseline = capture_baseline(
        lambda: (0, 0),
        lambda: (503, "no healthy target"),
        utc_now=clock.now,
    )
    assert baseline == ready_baseline(clock)


def test_baseline_rejects_nonzero_replicas():
    baseline = capture_baseline(lambda: (1, 1), lambda: (200, None))
    assert baseline["status"] == "invalid_nonzero_replicas"


def test_baseline_rejects_a_route_that_is_already_serving():
    baseline = capture_baseline(lambda: (0, 0), lambda: (200, None))
    assert baseline["status"] == "invalid_route_already_serving"


@pytest.mark.parametrize(
    "probe",
    [
        (None, "no AWS credentials available"),
        (403, "Forbidden"),
        (404, "wrong route"),
        (429, "throttled"),
    ],
)
def test_baseline_rejects_an_unproven_or_unauthorized_inference_probe(probe):
    baseline = capture_baseline(lambda: (0, 0), lambda: probe)
    assert baseline["status"] == "invalid_inference_probe"


def test_observer_records_ordered_scale_ready_inference_and_return_to_zero():
    states = iter([(0, 0), (1, 0), (1, 1), (1, 1), (0, 0)])
    inference = iter([(503, None), (503, None), (503, None), (200, None), (503, None)])
    clock = FakeClock()

    payload = observe_scale(
        load_started_at=clock.base,
        baseline=ready_baseline(clock),
        **load_guard(clock),
        read_replicas=lambda: next(states),
        probe_inference=lambda: next(inference),
        timeout_seconds=10,
        poll_seconds=1,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.now,
    )

    assert payload["status"] == "completed"
    assert list(payload["events"]) == [
        "scale_requested",
        "pod_ready",
        "first_inference_success",
        "returned_to_zero",
    ]
    assert payload["events"]["scale_requested"]["seconds_from_load_start"] == 1
    assert payload["events"]["pod_ready"]["seconds_from_load_start"] == 2
    assert payload["events"]["first_inference_success"]["seconds_from_load_start"] == 3
    assert payload["events"]["returned_to_zero"]["seconds_from_load_start"] == 4


def test_observer_checkpoints_observing_and_each_partial_transition():
    states = iter([(1, 0), (1, 1), (0, 0)])
    inference = iter([(503, None), (200, None), (503, None)])
    clock = FakeClock()
    checkpoints = []

    payload = observe_scale(
        load_started_at=clock.base,
        baseline=ready_baseline(clock),
        **load_guard(clock),
        read_replicas=lambda: next(states),
        probe_inference=lambda: next(inference),
        timeout_seconds=10,
        poll_seconds=1,
        checkpoint=lambda state: checkpoints.append(json.loads(json.dumps(state))),
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.now,
    )

    assert payload["status"] == "completed"
    assert checkpoints[0]["status"] == "observing"
    assert "scale_requested" in checkpoints[1]["events"]
    assert "pod_ready" in checkpoints[2]["events"]
    assert checkpoints[-1]["status"] == "completed"


def test_observer_ignores_transient_zero_until_bound_load_completes():
    states = iter([(1, 0), (1, 1), (1, 1), (0, 0), (1, 1), (0, 0)])
    inference = iter([(503, None), (503, None), (200, None), (503, None), (200, None), (503, None)])
    clock = FakeClock()
    load_states = iter(
        [
            bound_load_payload(clock, "running"),
            bound_load_payload(clock, "running"),
            bound_load_payload(clock, "running"),
            bound_load_payload(clock, "running"),
            bound_load_payload(clock, "completed"),
            bound_load_payload(clock, "completed"),
        ]
    )
    guard = load_guard(clock)
    guard["read_load_artifact"] = lambda: next(load_states)

    payload = observe_scale(
        load_started_at=clock.base,
        baseline=ready_baseline(clock),
        **guard,
        read_replicas=lambda: next(states),
        probe_inference=lambda: next(inference),
        timeout_seconds=10,
        poll_seconds=1,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.now,
    )

    assert payload["status"] == "completed"
    assert payload["events"]["returned_to_zero"]["seconds_from_load_start"] == 5
    assert payload["last_load_artifact"]["status"] == "completed"


def test_observer_fails_promptly_when_bound_load_fails():
    clock = FakeClock()
    load_states = iter(
        [
            bound_load_payload(clock, "running"),
            bound_load_payload(clock, "failed"),
        ]
    )
    guard = load_guard(clock)
    guard["read_load_artifact"] = lambda: next(load_states)
    replica_reads = []

    payload = observe_scale(
        load_started_at=clock.base,
        baseline=ready_baseline(clock),
        **guard,
        read_replicas=lambda: replica_reads.append(True) or (1, 0),
        probe_inference=lambda: (503, None),
        timeout_seconds=10,
        poll_seconds=1,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.now,
    )

    assert payload["status"] == "load_failed"
    assert payload["last_load_artifact"]["error"] == "RuntimeError: injected load failure"
    assert replica_reads == [True]
    assert clock.seconds == 1


def test_observer_fails_promptly_on_an_unexpected_terminal_load_status():
    clock = FakeClock()
    replica_reads = []
    payload = observe_scale(
        load_started_at=clock.base,
        baseline=ready_baseline(clock),
        **load_guard(clock, bound_load_payload(clock, "interrupted")),
        read_replicas=lambda: replica_reads.append(True) or (0, 0),
        probe_inference=lambda: (503, None),
        timeout_seconds=10,
        poll_seconds=1,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.now,
    )

    assert payload["status"] == "invalid_load_status"
    assert payload["last_load_artifact"]["status"] == "interrupted"
    assert replica_reads == []
    assert clock.seconds == 0


@pytest.mark.parametrize(
    ("field", "value", "error_fragment"),
    [
        ("run_id", OTHER_RUN_ID, "run_id"),
        ("load_artifact", "/tmp/other-load.json", "self-binding"),
        ("observer_ready_path", "/tmp/other-observer.json", "different observer"),
        ("ended_at", None, "ISO-8601"),
        ("ended_at", "2026-07-12T23:59:59Z", "predates"),
    ],
)
def test_observer_rejects_unbound_or_unfinished_completed_load(
    field,
    value,
    error_fragment,
):
    clock = FakeClock()
    load_payload = bound_load_payload(clock)
    if value is None:
        load_payload.pop(field)
    else:
        load_payload[field] = value
    replica_reads = []

    payload = observe_scale(
        load_started_at=clock.base,
        baseline=ready_baseline(clock),
        **load_guard(clock, load_payload),
        read_replicas=lambda: replica_reads.append(True) or (0, 0),
        probe_inference=lambda: (503, None),
        timeout_seconds=10,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.now,
    )

    assert payload["status"] == "invalid_load_artifact"
    assert error_fragment in payload["load_error"]
    assert replica_reads == []


def test_observer_rejects_a_200_before_the_eks_pod_is_ready():
    clock = FakeClock()
    payload = observe_scale(
        load_started_at=clock.base,
        baseline=ready_baseline(clock),
        **load_guard(clock),
        read_replicas=lambda: (0, 0),
        probe_inference=lambda: (200, None),
        timeout_seconds=1,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.now,
    )
    assert payload["status"] == "invalid_inference_success_before_pod_ready"
    assert payload["events"] == {}


def test_observer_does_not_count_an_invalid_200_response_as_inference_success():
    clock = FakeClock()
    payload = observe_scale(
        load_started_at=clock.base,
        baseline=ready_baseline(clock),
        **load_guard(clock),
        read_replicas=lambda: (1, 1),
        probe_inference=lambda: (200, "scorer response mmsi must equal 200000099"),
        timeout_seconds=1,
        poll_seconds=1,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.now,
    )
    assert payload["status"] == "timeout"
    assert "first_inference_success" not in payload["events"]
    assert payload["last_sample"]["inference_http_status"] == 200
    assert payload["last_sample"]["inference_error"]


def test_observer_rejects_an_invalid_baseline_without_probing():
    probes = []
    clock = FakeClock()
    baseline = {**ready_baseline(clock), "status": "invalid_nonzero_replicas"}
    payload = observe_scale(
        load_started_at=clock.base,
        baseline=baseline,
        **load_guard(clock),
        read_replicas=lambda: (0, 0),
        probe_inference=lambda: probes.append(True),
        timeout_seconds=1,
    )
    assert payload["status"] == "invalid_baseline"
    assert probes == []


def test_observer_times_out_with_partial_evidence():
    clock = FakeClock()
    payload = observe_scale(
        load_started_at=clock.base,
        baseline=ready_baseline(clock),
        **load_guard(clock, bound_load_payload(clock, "running")),
        read_replicas=lambda: (0, 0),
        probe_inference=lambda: (503, "not ready"),
        timeout_seconds=2,
        poll_seconds=1,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.now,
    )
    assert payload["status"] == "timeout"
    assert payload["events"] == {}
    assert payload["last_sample"]["inference_http_status"] == 503


def test_wait_for_load_start_requires_a_fresh_running_marker(tmp_path):
    path = tmp_path / "load.json"
    observer = tmp_path / "scale.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "status": "running",
                "run_id": RUN_ID,
                "load_artifact": str(path.resolve()),
                "observer_ready_path": str(observer.resolve()),
                "started_at": "2026-07-13T04:00:01.123456Z",
            }
        )
    )
    baseline = datetime(2026, 7, 13, 4, 0, tzinfo=UTC)
    assert wait_for_load_start(
        path,
        not_before=baseline,
        expected_run_id=RUN_ID,
        expected_observer_path=observer,
    ) == datetime(2026, 7, 13, 4, 0, 1, 123456, tzinfo=UTC)


@pytest.mark.parametrize(
    "url",
    [
        "http://abc.execute-api.us-east-1.amazonaws.com/v1/score-ais",
        "https://example.com/v1/score-ais",
        "https://abc.execute-api.us-west-2.amazonaws.com/v1/score-ais",
        "https://user@abc.execute-api.us-east-1.amazonaws.com/v1/score-ais",
        "https://abc.execute-api.us-east-1.amazonaws.com/healthz",
        "https://abc.execute-api.us-east-1.amazonaws.com/v1/score-ais?debug=1",
        "https://abc.execute-api.us-east-1.amazonaws.com/v1/score-ais#fragment",
    ],
)
def test_api_gateway_url_validation_rejects_unsafe_destinations(url):
    with pytest.raises(ValueError, match="exact regional HTTPS"):
        validate_api_gateway_url(url, "us-east-1")


def test_api_gateway_url_validation_accepts_the_regional_invoke_host():
    url = "https://abc123.execute-api.us-east-1.amazonaws.com/v1/score-ais"
    assert validate_api_gateway_url(url, "us-east-1") == url


def test_signed_inference_requires_a_valid_matching_ais_score_response(monkeypatch):
    import botocore.session
    from botocore.credentials import Credentials

    import scripts.observe_phase5_scale as mod

    class Session:
        @staticmethod
        def get_credentials():
            return Credentials("AKIDEXAMPLE", "secret", "token")

    class Response:
        status = 200

        def __init__(self, body):
            self.body = body

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return self.body

    def response_body(mmsi):
        return json.dumps(
            {
                "mmsi": mmsi,
                "score": 0.1,
                "confidence": 0.9,
                "reasons": [],
                "hitl_required": False,
                "trace_id": "trace-1",
                "latency_ms": 2.0,
                "n_history": 1,
            }
        ).encode()

    monkeypatch.setattr(botocore.session, "get_session", Session)
    opened = []
    monkeypatch.setattr(
        mod,
        "open_no_redirect",
        lambda request, timeout_seconds: (
            opened.append((request, timeout_seconds)) or Response(response_body(mod.INFERENCE_MMSI))
        ),
    )
    url = "https://abc.execute-api.us-east-1.amazonaws.com/v1/score-ais"
    assert signed_inference_status(url, "us-east-1") == (200, None)
    assert len(opened) == 1

    monkeypatch.setattr(
        mod,
        "open_no_redirect",
        lambda request, timeout_seconds: Response(response_body(mod.INFERENCE_MMSI + 1)),
    )
    status, error = signed_inference_status(url, "us-east-1")
    assert status == 200
    assert "must equal" in error


def test_live_inference_probe_allows_api_gateway_no_target_timeout(monkeypatch):
    import scripts.observe_phase5_scale as mod

    observed = {}

    def fake_probe(url, region, *, timeout_seconds):
        observed.update(url=url, region=region, timeout_seconds=timeout_seconds)
        return 503, "no healthy target"

    monkeypatch.setattr(mod, "signed_inference_status", fake_probe)
    url = "https://abc.execute-api.us-east-1.amazonaws.com/v1/score-ais"
    assert bounded_inference_status(url, "us-east-1") == (503, "no healthy target")
    assert observed == {
        "url": url,
        "region": "us-east-1",
        "timeout_seconds": INFERENCE_TIMEOUT_SECONDS,
    }


def test_deployment_read_has_kubectl_and_subprocess_timeouts(monkeypatch):
    calls = {}

    def fake_run(command, **kwargs):
        calls["command"] = command
        calls["kwargs"] = kwargs
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps({"spec": {"replicas": 2}, "status": {"availableReplicas": 1}}),
        )

    monkeypatch.setattr(subprocess, "run", fake_run)
    assert deployment_replicas("hm-serving", "serving", timeout_seconds=7) == (2, 1)
    assert "--request-timeout=7s" in calls["command"]
    assert calls["kwargs"]["timeout"] == 8


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf")])
def test_observer_timing_controls_must_be_finite_and_positive(value):
    with pytest.raises(ValueError, match="finite and > 0"):
        validate_positive_finite(value, "poll_seconds")


def test_wait_for_load_start_rejects_a_different_observer_binding(tmp_path):
    path = tmp_path / "load.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "status": "running",
                "run_id": RUN_ID,
                "load_artifact": str(path.resolve()),
                "observer_ready_path": str((tmp_path / "other-scale.json").resolve()),
                "started_at": "2026-07-13T04:00:01Z",
            }
        )
    )
    with pytest.raises(ValueError, match="different observer"):
        wait_for_load_start(
            path,
            not_before=datetime(2026, 7, 13, 4, 0, tzinfo=UTC),
            expected_run_id=RUN_ID,
            expected_observer_path=tmp_path / "scale.json",
        )


def test_main_persists_observing_and_preserves_partial_evidence_on_exception(monkeypatch, tmp_path):
    import scripts.observe_phase5_scale as mod

    output = tmp_path / "scale.json"
    load_artifact = tmp_path / "load.json"
    baseline = ready_baseline(FakeClock())
    load_started_at = datetime(2026, 7, 13, 0, 0, 1, tzinfo=UTC)
    monkeypatch.setattr(mod, "capture_baseline", lambda *_args, **_kwargs: baseline)
    monkeypatch.setattr(
        mod,
        "wait_for_load_start",
        lambda *_args, **_kwargs: load_started_at,
    )

    def fail_after_partial_checkpoint(**kwargs):
        assert json.loads(output.read_text())["status"] == "observing"
        kwargs["checkpoint"](
            {
                "schema_version": 1,
                "status": "observing",
                "load_started_at": load_started_at.isoformat().replace("+00:00", "Z"),
                "baseline": baseline,
                "poll_seconds": 1.0,
                "events": {"scale_requested": {"desired_replicas": 1}},
            }
        )
        raise RuntimeError("credential refresh failed")

    monkeypatch.setattr(mod, "observe_scale", fail_after_partial_checkpoint)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(Path(mod.__file__)),
            "--api-url",
            "https://abc.execute-api.us-east-1.amazonaws.com",
            "--load-artifact",
            str(load_artifact),
            "--output",
            str(output),
            "--run-id",
            RUN_ID,
        ],
    )

    assert mod.main() == 1
    payload = json.loads(output.read_text())
    assert payload["status"] == "observer_error"
    assert payload["baseline"] == baseline
    assert payload["events"]["scale_requested"]["desired_replicas"] == 1
    assert payload["error"] == "RuntimeError: credential refresh failed"


def test_observer_direct_script_help_smoke():
    script = Path(__file__).parents[2] / "scripts" / "observe_phase5_scale.py"
    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "--load-artifact" in result.stdout
    assert "--run-id" in result.stdout
