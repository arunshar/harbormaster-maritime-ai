"""Local tests for the W4 Flink 1.20 backpressure evidence helper."""

from __future__ import annotations

import argparse
import http.cookiejar
import json
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import scripts.capture_flink_backpressure as mod
from scripts.capture_flink_backpressure import (
    CaptureSettings,
    DashboardSession,
    EvidenceError,
    HTTPSOnlyRedirectHandler,
    LoadRun,
    RejectRedirectHandler,
    capture_backpressure,
    collect_sample,
    discover_running_job,
    parse_vertex_backpressure,
    resolve_capture_settings,
    summarize_samples,
    validate_authorization_url,
    validate_load_payload,
    wait_for_fresh_load,
)

RUN_ID = "12345678-1234-4234-9234-123456789abc"
JOB_ID = "a" * 32
VERTEX_A = "b" * 32
VERTEX_B = "c" * 32
START = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)


class FakeResponse:
    def __init__(self, payload=b"{}", *, url="https://dashboard.example/", status=200):
        self.payload = payload
        self.url = url
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def geturl(self):
        return self.url

    def getcode(self):
        return self.status

    def read(self, _limit=-1):
        return self.payload


class FakeOpener:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def open(self, request, timeout):
        self.requests.append((request, timeout))
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


class RouteClient:
    def __init__(self, routes):
        self.routes = routes
        self.paths = []
        self.origin = "https://dashboard.example"
        self.api_base_path = ""

    def get_json(self, path):
        self.paths.append(path)
        value = self.routes[path]
        return value() if callable(value) else value


class FakeClock:
    def __init__(self, start=START):
        self.start = start
        self.seconds = 0.0

    def monotonic(self):
        return self.seconds

    def sleep(self, seconds):
        self.seconds += seconds

    def utc_now(self):
        return self.start + timedelta(seconds=self.seconds)


def load_payload(path: Path, **overrides):
    payload = {
        "schema_version": 1,
        "status": "running",
        "run_id": RUN_ID,
        "load_artifact": str(path.resolve()),
        "started_at": START.isoformat().replace("+00:00", "Z"),
        "duration_seconds": 12.0,
        "profile": {
            "steady_rps": 2.0,
            "burst_rps": 20.0,
            "burst_start_s": 2.0,
            "burst_end_s": 4.0,
            "ramp_s": 1.0,
        },
    }
    payload.update(overrides)
    return payload


def load_run(path=Path("/tmp/load.json")):
    return LoadRun(
        run_id=RUN_ID,
        artifact_path=path.resolve(),
        started_at=START,
        duration_seconds=12.0,
        steady_rps=2.0,
        burst_rps=20.0,
        burst_start_seconds=2.0,
        burst_end_seconds=4.0,
        ramp_seconds=1.0,
    )


def backpressure_payload(ratio=0.0, level="ok"):
    return {
        "status": "ok",
        "backpressure-level": level,
        "end-timestamp": 1_752_408_000_000,
        "subtasks": [
            {
                "subtask": 0,
                "attempt-number": 0,
                "backpressure-level": level,
                "ratio": ratio,
                "busyRatio": 0.6,
                "idleRatio": 0.4 - ratio if ratio <= 0.4 else 0.0,
            }
        ],
    }


def running_routes(*, vertices=None, ratio=0.0, state="RUNNING"):
    vertices = vertices or [{"id": VERTEX_A, "name": "source"}]
    return {
        "/jobs/overview": {"jobs": [{"jid": JOB_ID, "state": "RUNNING"}]},
        f"/jobs/{JOB_ID}": {"jid": JOB_ID, "state": state, "vertices": vertices},
        **{
            f"/jobs/{JOB_ID}/vertices/{vertex['id']}/backpressure": backpressure_payload(ratio)
            for vertex in vertices
        },
    }


# ---------------------------------------------------------------------------
# Authorization and exact-origin HTTP boundary
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("url", "message"),
    [
        ("http://kinesisanalytics.us-east-1.amazonaws.com/x", "HTTPS"),
        ("https://example.com/x", "amazonaws.com"),
        ("https://user@example.amazonaws.com/x", "credentials"),
        ("https://example.amazonaws.com:8443/x", "standard HTTPS port"),
        ("https://example.amazonaws.com/x#fragment", "fragment"),
    ],
)
def test_authorization_url_rejects_unsafe_inputs(url, message):
    with pytest.raises(EvidenceError, match=message):
        validate_authorization_url(url)


def test_authorization_url_accepts_standard_aws_hosts():
    url = "https://kinesisanalytics.us-east-1.amazonaws.com/dashboard?token=secret"
    assert validate_authorization_url(url) == url
    china = "https://kinesisanalytics.cn-north-1.amazonaws.com.cn/dashboard?token=secret"
    assert validate_authorization_url(china) == china


def test_authorization_redirect_handler_refuses_https_downgrade():
    handler = HTTPSOnlyRedirectHandler()
    request = urllib.request.Request("https://service.amazonaws.com/start")
    with pytest.raises(EvidenceError, match="non-HTTPS"):
        handler.redirect_request(request, None, 302, "Found", {}, "http://dashboard/x")


def test_post_authorization_redirect_handler_refuses_every_redirect():
    handler = RejectRedirectHandler()
    request = urllib.request.Request("https://dashboard.example/jobs/overview")
    with pytest.raises(EvidenceError, match="redirect refused"):
        handler.redirect_request(request, None, 302, "Found", {}, "/login")


def test_authorize_shares_cookie_jar_but_only_auth_opener_allows_redirects(monkeypatch):
    final = "https://dashboard.example/flinkdashboard/?session=opaque"
    auth_opener = FakeOpener([FakeResponse(url=final)])
    api_opener = FakeOpener([])
    calls = []

    def fake_build_opener(*handlers):
        calls.append(handlers)
        return auth_opener if len(calls) == 1 else api_opener

    monkeypatch.setattr(mod.urllib.request, "build_opener", fake_build_opener)
    session = DashboardSession.authorize(
        "https://kinesisanalytics.us-east-1.amazonaws.com/presigned?token=secret", 3
    )
    assert session.origin == "https://dashboard.example"
    assert session.api_base_path == "/flinkdashboard"
    assert len(calls) == 2
    auth_cookie = next(
        handler for handler in calls[0] if isinstance(handler, urllib.request.HTTPCookieProcessor)
    )
    api_cookie = next(
        handler for handler in calls[1] if isinstance(handler, urllib.request.HTTPCookieProcessor)
    )
    assert isinstance(calls[0][1], HTTPSOnlyRedirectHandler)
    assert isinstance(calls[1][1], RejectRedirectHandler)
    assert isinstance(auth_cookie.cookiejar, http.cookiejar.CookieJar)
    assert auth_cookie.cookiejar is api_cookie.cookiejar
    request, timeout = auth_opener.requests[0]
    assert request.full_url.endswith("token=secret")
    assert timeout == 3


def test_authorize_reports_http_status_without_echoing_presigned_url(monkeypatch):
    error = urllib.error.HTTPError("https://aws.example/?secret=x", 403, "no", {}, None)
    opener = FakeOpener([error])
    monkeypatch.setattr(mod.urllib.request, "build_opener", lambda *handlers: opener)
    with pytest.raises(EvidenceError, match="HTTP 403") as raised:
        DashboardSession.authorize("https://service.amazonaws.com/?secret=x")
    assert "secret=x" not in str(raised.value)


def test_dashboard_get_json_is_get_to_exact_origin_and_aws_base_path():
    opener = FakeOpener(
        [
            FakeResponse(
                b'{"jobs": []}',
                url="https://dashboard.example/flinkdashboard/jobs/overview",
            )
        ]
    )
    client = DashboardSession("https://dashboard.example/ui", opener, 4, "/flinkdashboard")
    assert client.get_json("/jobs/overview") == {"jobs": []}
    request, timeout = opener.requests[0]
    assert request.full_url == "https://dashboard.example/flinkdashboard/jobs/overview"
    assert request.method == "GET"
    assert request.headers["Accept"] == "application/json"
    assert timeout == 4


@pytest.mark.parametrize(
    ("path", "message"),
    [
        ("/config", "not allowlisted"),
        ("//evil.example/jobs/overview", "not allowlisted"),
        ("/jobs/overview?x=1", "not allowlisted"),
    ],
)
def test_dashboard_get_json_rejects_non_allowlisted_paths(path, message):
    client = DashboardSession("https://dashboard.example", FakeOpener([]), 2)
    with pytest.raises(EvidenceError, match=message):
        client.get_json(path)


def test_dashboard_get_json_rejects_changed_response_origin():
    opener = FakeOpener([FakeResponse(b"{}", url="https://other.example/jobs/overview")])
    client = DashboardSession("https://dashboard.example", opener, 2)
    with pytest.raises(EvidenceError, match="changed origin"):
        client.get_json("/jobs/overview")


def test_dashboard_get_json_rejects_changed_response_path():
    opener = FakeOpener([FakeResponse(b"{}", url="https://dashboard.example/jobs/overview")])
    client = DashboardSession("https://dashboard.example", opener, 2, "/flinkdashboard")
    with pytest.raises(EvidenceError, match="changed path"):
        client.get_json("/jobs/overview")


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"not-json", "malformed JSON"),
        (b"[]", "JSON object"),
        (b"{" + b'"x":"' + b"x" * mod.MAX_JSON_BYTES + b'"}', "size limit"),
    ],
)
def test_dashboard_get_json_fails_closed_on_invalid_bodies(payload, message):
    opener = FakeOpener([FakeResponse(payload, url="https://dashboard.example/jobs/overview")])
    client = DashboardSession("https://dashboard.example", opener, 2)
    with pytest.raises(EvidenceError, match=message):
        client.get_json("/jobs/overview")


# ---------------------------------------------------------------------------
# Load artifact binding and timing
# ---------------------------------------------------------------------------


def test_validate_load_payload_derives_bounds_from_fresh_bound_run(tmp_path):
    path = tmp_path / "load.json"
    run = validate_load_payload(
        load_payload(path),
        path,
        RUN_ID,
        now=START + timedelta(seconds=1),
        max_start_age_seconds=30,
    )
    assert run.run_id == RUN_ID
    assert run.artifact_path == path.resolve()
    assert run.burst_start_seconds == 2
    assert run.burst_window_end_seconds == 5
    assert run.profile["burst_rps"] == 20


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda payload, _path: payload.update(run_id="22345678-1234-4234-9234-123456789abc"),
            "run_id",
        ),
        (
            lambda payload, path: payload.update(load_artifact=str(path / "other.json")),
            "different path",
        ),
        (lambda payload, _path: payload.update(status="completed"), "must be running"),
        (lambda payload, _path: payload.update(started_at="not-a-time"), "ISO-8601"),
        (lambda payload, _path: payload.update(profile={}), "steady_rps"),
        (lambda payload, _path: payload.update(duration_seconds=float("inf")), "finite"),
    ],
)
def test_validate_load_payload_rejects_mismatch_or_malformed_data(tmp_path, mutate, message):
    path = tmp_path / "load.json"
    payload = load_payload(path)
    mutate(payload, tmp_path)
    with pytest.raises(EvidenceError, match=message):
        validate_load_payload(
            payload,
            path,
            RUN_ID,
            now=START + timedelta(seconds=1),
            max_start_age_seconds=30,
        )


def test_validate_load_payload_rejects_stale_or_too_late_runs(tmp_path):
    path = tmp_path / "load.json"
    with pytest.raises(EvidenceError, match="stale"):
        validate_load_payload(
            load_payload(path),
            path,
            RUN_ID,
            now=START + timedelta(seconds=31),
            max_start_age_seconds=30,
        )
    with pytest.raises(EvidenceError, match="pre-burst"):
        validate_load_payload(
            load_payload(path),
            path,
            RUN_ID,
            now=START + timedelta(seconds=2),
            max_start_age_seconds=30,
        )


def test_wait_for_fresh_load_accepts_preparing_then_running(tmp_path):
    path = tmp_path / "load.json"
    preparing = {
        "schema_version": 1,
        "status": "preparing",
        "run_id": RUN_ID,
        "load_artifact": str(path.resolve()),
    }
    values = iter([preparing, load_payload(path)])
    clock = FakeClock()
    run = wait_for_fresh_load(
        path,
        RUN_ID,
        timeout_seconds=3,
        max_start_age_seconds=30,
        reader=lambda _: next(values),
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.utc_now,
    )
    assert run.run_id == RUN_ID
    assert clock.seconds == pytest.approx(0.2)


def test_wait_for_fresh_load_rejects_malformed_json(tmp_path):
    path = tmp_path / "load.json"
    path.write_text("not-json")
    with pytest.raises(EvidenceError, match="malformed JSON"):
        wait_for_fresh_load(path, RUN_ID, timeout_seconds=1)


# ---------------------------------------------------------------------------
# Flink 1.20 schema parsing and sampling
# ---------------------------------------------------------------------------


def test_discover_running_job_selects_exactly_one():
    client = RouteClient(
        {
            "/jobs/overview": {
                "jobs": [
                    {"jid": "d" * 32, "state": "FINISHED"},
                    {"jid": JOB_ID, "state": "RUNNING"},
                ]
            }
        }
    )
    assert discover_running_job(client) == JOB_ID


@pytest.mark.parametrize(
    "jobs",
    [
        [],
        [{"jid": JOB_ID, "state": "FINISHED"}],
        [{"jid": JOB_ID, "state": "RUNNING"}, {"jid": "d" * 32, "state": "RUNNING"}],
    ],
)
def test_discover_running_job_rejects_zero_or_ambiguous_running_jobs(jobs):
    with pytest.raises(EvidenceError, match="exactly one RUNNING"):
        discover_running_job(RouteClient({"/jobs/overview": {"jobs": jobs}}))


def test_parse_vertex_backpressure_preserves_raw_levels_and_ratios():
    payload = backpressure_payload(0.72, "high")
    payload["backpressureLevel"] = "high"
    payload["subtasks"][0]["backpressureLevel"] = "high"
    parsed = parse_vertex_backpressure(payload, VERTEX_A, "inference sink")
    assert parsed == {
        "vertex_id": VERTEX_A,
        "vertex_name": "inference sink",
        "backpressure_status": "ok",
        "backpressure_level": "high",
        "max_ratio": 0.72,
        "subtasks": [
            {
                "subtask": 0,
                "backpressure_level": "high",
                "ratio": 0.72,
                "attempt_number": 0,
                "busy_ratio": 0.6,
                "idle_ratio": 0.0,
            }
        ],
        "end_timestamp_ms": 1_752_408_000_000,
    }


@pytest.mark.parametrize(
    ("busy", "idle", "busy_reason", "idle_reason"),
    [
        (float("nan"), float("inf"), "non_finite", "non_finite"),
        ("NaN", None, "non_finite", "null"),
    ],
)
def test_parse_vertex_backpressure_tolerates_unavailable_diagnostic_ratios(
    busy, idle, busy_reason, idle_reason
):
    payload = backpressure_payload(0.4, "low")
    payload["subtasks"][0]["busyRatio"] = busy
    payload["subtasks"][0]["idleRatio"] = idle
    parsed = parse_vertex_backpressure(payload, VERTEX_A, "source")
    subtask = parsed["subtasks"][0]
    assert subtask["ratio"] == 0.4
    assert subtask["busy_ratio"] is None
    assert subtask["busy_ratio_unavailable"] == busy_reason
    assert subtask["idle_ratio"] is None
    assert subtask["idle_ratio_unavailable"] == idle_reason


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda payload: payload.update(status="deprecated"), "not ok"),
        (lambda payload: payload.update(subtasks=[]), "no subtasks"),
        (lambda payload: payload["subtasks"][0].update(ratio=1.1), r"must be in \[0, 1\]"),
        (lambda payload: payload.update(backpressureLevel="high"), "aliases disagree"),
        (lambda payload: payload["subtasks"][0].update(ratio="0.5"), "must be a number"),
    ],
)
def test_parse_vertex_backpressure_rejects_unusable_observations(mutate, message):
    payload = backpressure_payload(0.0)
    mutate(payload)
    with pytest.raises(EvidenceError, match=message):
        parse_vertex_backpressure(payload, VERTEX_A, "source")


def test_collect_sample_queries_all_vertices_and_retains_job_state():
    vertices = [
        {"id": VERTEX_A, "name": "source"},
        {"id": VERTEX_B, "name": "sink"},
    ]
    routes = running_routes(vertices=vertices)
    routes[f"/jobs/{JOB_ID}/vertices/{VERTEX_B}/backpressure"] = backpressure_payload(0.75, "high")
    client = RouteClient(routes)
    sample = collect_sample(
        client,
        JOB_ID,
        captured_at=START,
        seconds_from_load_start=1.25,
    )
    assert sample["captured_at"] == "2026-07-13T12:00:00Z"
    assert sample["seconds_from_load_start"] == 1.25
    assert sample["job_state"] == "RUNNING"
    assert sample["vertex_count"] == 2
    assert sample["max_ratio"] == 0.75
    assert client.paths == [
        f"/jobs/{JOB_ID}",
        f"/jobs/{JOB_ID}/vertices/{VERTEX_A}/backpressure",
        f"/jobs/{JOB_ID}/vertices/{VERTEX_B}/backpressure",
    ]


def test_collect_sample_retries_deprecated_sampling_status_within_one_deadline():
    clock = FakeClock()
    responses = iter([{"status": "deprecated"}, backpressure_payload(0.4, "low")])
    routes = running_routes()
    routes[f"/jobs/{JOB_ID}/vertices/{VERTEX_A}/backpressure"] = lambda: next(responses)
    client = RouteClient(routes)
    result = collect_sample(
        client,
        JOB_ID,
        captured_at=START,
        seconds_from_load_start=0,
        backpressure_ready_timeout_seconds=1,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    assert result["max_ratio"] == 0.4
    assert clock.seconds == pytest.approx(0.2)
    assert client.paths.count(f"/jobs/{JOB_ID}/vertices/{VERTEX_A}/backpressure") == 2


def test_collect_sample_fails_if_deprecated_status_never_becomes_ready():
    clock = FakeClock()
    routes = running_routes()
    routes[f"/jobs/{JOB_ID}/vertices/{VERTEX_A}/backpressure"] = {"status": "deprecated"}
    with pytest.raises(EvidenceError, match="did not become ready within 1.0s"):
        collect_sample(
            RouteClient(routes),
            JOB_ID,
            captured_at=START,
            seconds_from_load_start=0,
            backpressure_ready_timeout_seconds=1,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )
    assert clock.seconds == pytest.approx(1.0)


def test_collect_sample_fails_closed_if_job_not_running_or_has_no_vertices():
    with pytest.raises(EvidenceError, match="not RUNNING"):
        collect_sample(
            RouteClient(running_routes(state="FAILED")),
            JOB_ID,
            captured_at=START,
            seconds_from_load_start=0,
        )
    routes = running_routes()
    routes[f"/jobs/{JOB_ID}"]["vertices"] = []
    with pytest.raises(EvidenceError, match="no vertices"):
        collect_sample(
            RouteClient(routes),
            JOB_ID,
            captured_at=START,
            seconds_from_load_start=0,
        )


# ---------------------------------------------------------------------------
# Bounded capture, interval summaries, and evidence lifecycle
# ---------------------------------------------------------------------------


def sample(seconds, ratio, vertex_id=VERTEX_A):
    return {
        "captured_at": mod._utc_text(START + timedelta(seconds=seconds)),
        "seconds_from_load_start": seconds,
        "job_id": JOB_ID,
        "job_state": "RUNNING",
        "vertex_count": 1,
        "max_ratio": ratio,
        "vertices": [
            {
                "vertex_id": vertex_id,
                "vertex_name": "source",
                "backpressure_status": "ok",
                "backpressure_level": "high" if ratio > 0.5 else "ok",
                "max_ratio": ratio,
                "subtasks": [{"subtask": 0, "backpressure_level": "ok", "ratio": ratio}],
            }
        ],
    }


def test_summarize_samples_separates_intervals_and_evaluates_conditions():
    summary = summarize_samples(
        [
            sample(0, 0.7),
            sample(0.5, 0.2),
            sample(1, 0.05),
            sample(1.5, 0.04),
            sample(1.9, 0.1),
            sample(2, 0.11),
            sample(5, 0.8),
            sample(6, 0.4),
            sample(7, 0.05),
            sample(8, 0.02),
            sample(9, 0),
        ],
        load_run(),
    )
    assert summary["pre_burst"]["sample_count"] == 5
    assert summary["pre_burst"]["max_ratio"] == 0.7
    assert summary["pre_burst"]["tail_max_ratio"] == 0.1
    assert summary["pre_burst"]["first_resolved_at"] == "2026-07-13T12:00:01Z"
    assert summary["burst"]["sample_count"] == 2
    assert summary["burst"]["max_ratio"] == 0.8
    assert summary["post_burst_recovery"]["sample_count"] == 4
    assert summary["post_burst_recovery"]["max_ratio"] == 0.4
    assert summary["post_burst_recovery"]["tail_max_ratio"] == 0.05
    assert summary["post_burst_recovery"]["first_resolved_at"] == "2026-07-13T12:00:07Z"
    assert summary["evaluated_conditions"] == {
        "pre_burst_tail_max_ratio_lte_0_1": True,
        "burst_max_ratio_gt_0_1": True,
        "post_burst_tail_max_ratio_lte_0_1": True,
        "all_conditions_true": True,
    }
    assert "pass" not in json.dumps(summary).lower()


def test_summarize_samples_reports_false_conditions_without_a_pass_label():
    summary = summarize_samples(
        [
            sample(0, 0.2),
            sample(0.5, 0.2),
            sample(1, 0.2),
            sample(3, 0.1),
            sample(6, 0.2),
            sample(7, 0.2),
            sample(8, 0.2),
        ],
        load_run(),
    )
    assert summary["evaluated_conditions"]["pre_burst_tail_max_ratio_lte_0_1"] is False
    assert summary["evaluated_conditions"]["burst_max_ratio_gt_0_1"] is False
    assert summary["evaluated_conditions"]["post_burst_tail_max_ratio_lte_0_1"] is False
    assert summary["pre_burst"]["first_resolved_at"] is None
    assert summary["post_burst_recovery"]["first_resolved_at"] is None
    assert "pass" not in json.dumps(summary).lower()


@pytest.mark.parametrize(
    ("samples", "message"),
    [
        ([], "no Flink"),
        (
            [sample(3, 0.2), sample(6, 0.0), sample(7, 0.0), sample(8, 0.0)],
            "pre-burst",
        ),
        (
            [
                sample(0, 0.0),
                sample(0.5, 0.0),
                sample(1, 0.0),
                sample(6, 0.0),
                sample(7, 0.0),
                sample(8, 0.0),
            ],
            "burst interval",
        ),
        (
            [
                sample(0, 0.0),
                sample(0.5, 0.0),
                sample(1, 0.0),
                sample(3, 0.2),
            ],
            "recovery interval",
        ),
        (
            [
                sample(0, 0.0),
                sample(0.5, 0.0),
                sample(1, 0.0),
                sample(3, 0.2),
                sample(6, 0.0),
                sample(7, 0.0),
            ],
            "post-burst recovery",
        ),
        (
            [
                sample(0, 0.0),
                sample(1, 0.0),
                sample(3, 0.2),
                sample(6, 0.0),
                sample(7, 0.0),
                sample(8, 0.0),
            ],
            "pre-burst interval",
        ),
    ],
)
def test_summarize_samples_fails_closed_without_required_samples(samples, message):
    with pytest.raises(EvidenceError, match=message):
        summarize_samples(samples, load_run())


def test_resolve_capture_settings_defaults_to_load_and_requires_full_burst():
    run = load_run()
    assert resolve_capture_settings(run, 0.5, None, current_age_seconds=0) == CaptureSettings(
        0.5, 12
    )
    assert resolve_capture_settings(run, 0.5, 6.5, current_age_seconds=0) == CaptureSettings(
        0.5, 6.5
    )
    with pytest.raises(EvidenceError, match="three post-burst"):
        resolve_capture_settings(run, 0.5, 6.4, current_age_seconds=0)
    with pytest.raises(EvidenceError, match="load duration"):
        resolve_capture_settings(run, 0.5, 13, current_age_seconds=0)
    with pytest.raises(EvidenceError, match="three pre-burst"):
        resolve_capture_settings(run, 0.5, 6.5, current_age_seconds=0.6)


def test_capture_backpressure_polls_through_burst_end_and_updates_progress():
    clock = FakeClock()

    def current_backpressure():
        ratio = 0.05 if clock.seconds < 2 or clock.seconds > 5 else 0.7
        return backpressure_payload(ratio, "high" if ratio > 0.5 else "ok")

    routes = running_routes()
    routes[f"/jobs/{JOB_ID}/vertices/{VERTEX_A}/backpressure"] = current_backpressure
    client = RouteClient(routes)
    progress = []
    job_id, samples, summary = capture_backpressure(
        client,
        load_run(),
        CaptureSettings(0.5, 6.5),
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.utc_now,
        on_progress=lambda jid, current: progress.append((jid, len(current))),
    )
    assert job_id == JOB_ID
    assert [item["seconds_from_load_start"] for item in samples] == pytest.approx(
        [index / 2 for index in range(14)]
    )
    assert progress[0] == (JOB_ID, 1)
    assert progress[-1] == (JOB_ID, 14)
    assert len(progress) == 14
    assert summary["evaluated_conditions"]["all_conditions_true"] is True


def cli_args(tmp_path, **overrides):
    values = {
        "dashboard_url": "https://service.amazonaws.com/presigned?token=secret",
        "load_artifact": str(tmp_path / "load.json"),
        "expected_run_id": RUN_ID,
        "artifact": str(tmp_path / "backpressure.json"),
        "poll_interval_seconds": 0.5,
        "duration_seconds": 6.5,
        "request_timeout_seconds": 2.0,
        "backpressure_ready_timeout_seconds": 2.0,
        "load_wait_timeout_seconds": 2.0,
        "max_load_start_age_seconds": 30.0,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def test_execute_persists_completed_atomic_evidence(monkeypatch, tmp_path):
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return START.astimezone(tz) if tz is not None else START.replace(tzinfo=None)

    args = cli_args(tmp_path)
    run = load_run(Path(args.load_artifact))
    samples = [
        sample(0, 0.02),
        sample(0.5, 0.02),
        sample(1, 0.02),
        sample(3, 0.6),
        sample(5.5, 0.02),
        sample(6, 0.02),
        sample(6.5, 0.02),
    ]
    summary = summarize_samples(samples, run)
    client = RouteClient({})
    client.origin = "https://dashboard.example"
    monkeypatch.setattr(mod, "datetime", FrozenDateTime)
    monkeypatch.setattr(mod.DashboardSession, "authorize", lambda *_: client)
    monkeypatch.setattr(mod, "wait_for_fresh_load", lambda *a, **k: run)
    monkeypatch.setattr(
        mod,
        "capture_backpressure",
        lambda *a, on_progress, **k: (JOB_ID, samples, summary),
    )
    assert mod.execute(args) == 0
    payload = json.loads(Path(args.artifact).read_text())
    assert payload["status"] == "completed"
    assert payload["run_id"] == RUN_ID
    assert payload["dashboard_origin"] == "https://dashboard.example"
    assert payload["dashboard_api_base_path"] == ""
    assert payload["sample_count"] == 7
    assert payload["summary"] == summary
    assert "token=secret" not in Path(args.artifact).read_text()


def test_execute_persists_failed_evidence_and_never_overwrites(monkeypatch, tmp_path):
    args = cli_args(tmp_path)
    client = RouteClient({})
    client.origin = "https://dashboard.example"
    monkeypatch.setattr(mod.DashboardSession, "authorize", lambda *_: client)
    monkeypatch.setattr(
        mod,
        "wait_for_fresh_load",
        lambda *a, **k: (_ for _ in ()).throw(EvidenceError("malformed load")),
    )
    assert mod.execute(args) == 1
    payload = json.loads(Path(args.artifact).read_text())
    assert payload["status"] == "failed"
    assert payload["sample_count"] == 0
    assert payload["error"] == "EvidenceError: malformed load"
    with pytest.raises(EvidenceError, match="already exists"):
        mod.execute(args)


def test_main_requires_all_live_evidence_inputs(monkeypatch, capsys):
    for name in (
        "FLINK_DASHBOARD_URL",
        "W4_ARTIFACT_PATH",
        "W4_RUN_ID",
        "W4_FLINK_BACKPRESSURE_ARTIFACT_PATH",
    ):
        monkeypatch.delenv(name, raising=False)
    assert mod.main([]) == 2
    assert "FLINK_DASHBOARD_URL" in capsys.readouterr().err


def test_invalid_numeric_environment_default_exits_cleanly(monkeypatch, capsys):
    monkeypatch.setenv("FLINK_BACKPRESSURE_POLL_INTERVAL_S", "not-a-number")
    with pytest.raises(SystemExit) as raised:
        mod.main([])
    assert raised.value.code == 2
    captured = capsys.readouterr()
    assert "invalid float value" in captured.err
    assert "Traceback" not in captured.err
