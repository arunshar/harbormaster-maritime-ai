"""Unit tests for the Flink job's pure window logic (gate G5).

These functions were extracted verbatim out of job.py (which imports pyflink at
module top and so cannot be imported here) into the sibling window_logic module,
which imports NO pyflink. That extraction is what makes this suite possible: the
functions job.py runs per vessel are now importable and testable without a Flink
runtime or AWS. Tests are hermetic (no network, no clock, seeded, deterministic).
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from flink.window_logic import (
    P_PHYS_GATE,
    VESSEL_V_MAX_MPS,
    Fix,
    WindowFeatures,
    _RejectRedirects,
    feature_item,
    gap_since_last_s,
    haversine_m,
    open_no_redirect,
    passes_gate,
    post_scorer_with_retry,
    quarantine_envelope,
    sigv4_headers,
    v_required_mps,
    validate_ais_score_response,
    validate_execute_api_url,
    window_features,
)

T0 = datetime(2024, 6, 1, tzinfo=UTC)


def test_first_window_has_zero_inter_fix_features():
    # No previous fix (vessel's first window): the inter-fix features are all zero
    # and p_physical is a benign 1.0 (nothing to contradict). The current fix's own
    # instantaneous fields (sog/cog/heading) still pass through.
    wf = window_features(Fix(40.5, -73.9, T0, sog=10.0, cog=90.0, heading=88.0), None)
    assert wf.gap_since_last_s == 0.0
    assert wf.distance_m == 0.0
    assert wf.v_required_mps == 0.0
    assert wf.p_physical == 1.0
    assert (wf.sog, wf.cog, wf.heading) == (10.0, 90.0, 88.0)


def test_non_first_window_computes_expected_features():
    # A ~0.01 deg latitude step (~1112 m) over exactly 60 s: distance, gap, and the
    # required speed are all derivable in closed form, so assert the exact values
    # (not just inequalities). ~1112 m / 60 s ~ 18.5 m/s exceeds the 12.86 m/s cap,
    # so p_physical drops below 1.0 by exactly v_max / v_required.
    prev = Fix(40.50, -73.90, T0)
    curr = Fix(40.51, -73.90, T0 + timedelta(minutes=1))
    wf = window_features(curr, prev)

    expected_dist = haversine_m(40.50, -73.90, 40.51, -73.90)
    expected_gap = gap_since_last_s(T0, T0 + timedelta(minutes=1))
    expected_vreq = v_required_mps(expected_dist, expected_gap)

    assert wf.gap_since_last_s == 60.0
    assert wf.distance_m == pytest.approx(expected_dist)
    assert wf.v_required_mps == pytest.approx(expected_vreq)
    assert wf.v_required_mps > VESSEL_V_MAX_MPS
    assert wf.p_physical == pytest.approx(VESSEL_V_MAX_MPS / expected_vreq)
    assert wf.p_physical < 1.0


def test_passes_gate_at_above_and_below_threshold_boundary():
    # Build features whose p_physical sits exactly on, just above, and just below
    # the gate, and assert the >= boundary (at threshold passes).
    at = WindowFeatures(None, None, None, 0.0, 0.0, 0.0, P_PHYS_GATE)
    above = WindowFeatures(None, None, None, 0.0, 0.0, 0.0, P_PHYS_GATE + 1e-9)
    below = WindowFeatures(None, None, None, 0.0, 0.0, 0.0, P_PHYS_GATE - 1e-9)

    assert passes_gate(at) is True  # gate is inclusive (>=)
    assert passes_gate(above) is True
    assert passes_gate(below) is False

    # explicit threshold argument is honored the same way
    feats = WindowFeatures(None, None, None, 0.0, 0.0, 0.0, 0.5)
    assert passes_gate(feats, 0.5) is True
    assert passes_gate(feats, 0.5 + 1e-9) is False


def test_feature_item_shape_ttl_and_key_fields():
    feats = window_features(Fix(40.5, -73.9, T0, sog=10.0, cog=90.0, heading=88.0), None)
    item = feature_item(200000001, feats, T0, ttl_days=7)

    # key fields for the Feast online (DynamoDB) table
    assert item["entity_id"] == "200000001"
    assert item["feature_name"] == "window"
    assert item["t"] == "2024-06-01T00:00:00Z"
    # ttl is derived from the event timestamp + the retention window
    assert item["ttl"] == int(T0.timestamp()) + 7 * 86400
    # feature payload mirrors the WindowFeatures fields
    assert item["gap_since_last_s"] == feats.gap_since_last_s
    assert item["distance_m"] == feats.distance_m
    assert item["v_required_mps"] == feats.v_required_mps
    assert item["p_physical"] == feats.p_physical
    assert (item["sog"], item["cog"], item["heading"]) == (10.0, 90.0, 88.0)
    # exact key set (no stray fields leak into the item)
    assert set(item) == {
        "entity_id",
        "feature_name",
        "t",
        "gap_since_last_s",
        "distance_m",
        "v_required_mps",
        "p_physical",
        "sog",
        "cog",
        "heading",
        "ttl",
    }
    # ttl_days scales the retention window linearly
    assert feature_item(200000001, feats, T0, ttl_days=1)["ttl"] == int(T0.timestamp()) + 86400


def test_deterministic_on_repeated_calls():
    # Same inputs must yield identical outputs across repeated calls: the functions
    # are pure (no clock, no randomness, no shared mutable state).
    prev = Fix(40.50, -73.90, T0)
    curr = Fix(40.51, -73.90, T0 + timedelta(minutes=1))

    first = window_features(curr, prev)
    second = window_features(curr, prev)
    assert first == second  # frozen dataclass equality is field-wise

    item_a = feature_item(200000001, first, T0)
    item_b = feature_item(200000001, second, T0)
    assert item_a == item_b

    assert passes_gate(first) == passes_gate(second)


# --- Streaming robustness: quarantine (DLQ) envelope ------------------------


def test_quarantine_envelope_wraps_raw_reason_and_timestamp():
    env = quarantine_envelope('{"bad":true}', "parse_error: missing mmsi", T0)
    assert env["raw"] == '{"bad":true}'
    assert env["reason"] == "parse_error: missing mmsi"
    assert env["quarantined_at"] == "2024-06-01T00:00:00Z"


def test_quarantine_envelope_decodes_bytes_payload():
    env = quarantine_envelope(b'{"x":1}', "scorer_post_failed: boom", T0)
    assert env["raw"] == '{"x":1}'  # bytes decoded to text so the DLQ record is readable


def test_quarantine_envelope_handles_undecodable_bytes():
    # Invalid UTF-8 must not raise; the DLQ record preserves what it can (never lose
    # the fact that a bad record arrived) rather than crashing the operator.
    env = quarantine_envelope(b"\xff\xfe not utf8", "parse_error: bad bytes", T0)
    assert isinstance(env["raw"], str)
    assert "not utf8" in env["raw"]


def test_sigv4_headers_sign_execute_api_with_runtime_credentials(monkeypatch):
    import botocore.session
    from botocore.credentials import Credentials

    from flink import window_logic

    class Session:
        @staticmethod
        def get_credentials():
            return Credentials("AKIDEXAMPLE", "secret", "session-token")

    monkeypatch.setattr(botocore.session, "get_session", Session)
    window_logic._runtime_botocore_session.cache_clear()
    headers = sigv4_headers(
        "https://example.execute-api.us-east-1.amazonaws.com/v1/score-ais",
        b'{"mmsi":200000001}',
        "us-east-1",
    )
    assert headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/")
    assert "/us-east-1/execute-api/aws4_request" in headers["Authorization"]
    assert headers["X-Amz-Security-Token"] == "session-token"
    assert headers["X-Amz-Date"]


def test_sigv4_headers_fail_closed_without_runtime_credentials(monkeypatch):
    import botocore.session

    from flink import window_logic

    class Session:
        @staticmethod
        def get_credentials():
            return None

    monkeypatch.setattr(botocore.session, "get_session", Session)
    window_logic._runtime_botocore_session.cache_clear()
    with pytest.raises(RuntimeError, match="no AWS credentials"):
        sigv4_headers(
            "https://example.execute-api.us-east-1.amazonaws.com/v1/score-ais",
            b"{}",
            "us-east-1",
        )


def test_sigv4_rejects_an_unsafe_url_before_reading_credentials(monkeypatch):
    import botocore.session

    from flink import window_logic

    calls = []

    class Session:
        @staticmethod
        def get_credentials():
            calls.append("credentials")
            raise AssertionError("credential provider must not run for an unsafe URL")

    monkeypatch.setattr(botocore.session, "get_session", Session)
    window_logic._runtime_botocore_session.cache_clear()
    with pytest.raises(ValueError, match="exact regional HTTPS"):
        sigv4_headers("https://attacker.example/v1/score-ais", b"{}", "us-east-1")
    assert calls == []


def test_sigv4_headers_refresh_rotated_credentials_from_the_cached_session(monkeypatch):
    import botocore.session
    from botocore.credentials import Credentials

    from flink import window_logic

    credentials = iter(
        [
            Credentials("AKIDFIRST", "secret-1", "token-1"),
            Credentials("AKIDSECOND", "secret-2", "token-2"),
        ]
    )
    calls = {"sessions": 0, "credentials": 0}

    class Session:
        @staticmethod
        def get_credentials():
            calls["credentials"] += 1
            return next(credentials)

    def get_session():
        calls["sessions"] += 1
        return Session()

    monkeypatch.setattr(botocore.session, "get_session", get_session)
    window_logic._runtime_botocore_session.cache_clear()
    url = "https://example.execute-api.us-east-1.amazonaws.com/v1/score-ais"
    first = sigv4_headers(url, b"{}", "us-east-1")
    second = sigv4_headers(url, b"{}", "us-east-1")

    assert "Credential=AKIDFIRST/" in first["Authorization"]
    assert first["X-Amz-Security-Token"] == "token-1"
    assert "Credential=AKIDSECOND/" in second["Authorization"]
    assert second["X-Amz-Security-Token"] == "token-2"
    assert calls == {"sessions": 1, "credentials": 2}


@pytest.mark.parametrize(
    "url",
    [
        "http://abc.execute-api.us-east-1.amazonaws.com/v1/score-ais",
        "https://abc.execute-api.us-west-2.amazonaws.com/v1/score-ais",
        "https://abc.execute-api.us-east-1.amazonaws.com/healthz",
        "https://abc.execute-api.us-east-1.amazonaws.com/v1/score-ais?debug=1",
        "https://abc.execute-api.us-east-1.amazonaws.com/v1/score-ais#fragment",
        "https://abc.execute-api.us-east-1.amazonaws.com:443/v1/score-ais",
        "https://user@abc.execute-api.us-east-1.amazonaws.com/v1/score-ais",
    ],
)
def test_execute_api_url_validation_rejects_every_non_exact_destination(url):
    with pytest.raises(ValueError, match="exact regional HTTPS"):
        validate_execute_api_url(url, "us-east-1")


def test_execute_api_url_validation_accepts_the_exact_scoring_route():
    url = "https://abc.execute-api.us-east-1.amazonaws.com/v1/score-ais"
    assert validate_execute_api_url(url, "us-east-1") == url


def test_execute_api_url_validation_accepts_china_partition_suffix():
    url = "https://abc.execute-api.cn-north-1.amazonaws.com.cn/v1/score-ais"
    assert validate_execute_api_url(url, "cn-north-1") == url


@pytest.mark.parametrize("timeout_seconds", (0, -1, float("inf"), float("nan")))
def test_open_no_redirect_rejects_unbounded_timeout(timeout_seconds):
    request = urllib.request.Request("https://example.com")
    with pytest.raises(ValueError, match="finite and > 0"):
        open_no_redirect(request, timeout_seconds)


def test_open_no_redirect_uses_redirect_rejecting_opener(monkeypatch):
    calls = []
    handlers = []
    response = object()

    class Opener:
        @staticmethod
        def open(request, *, timeout):
            calls.append((request, timeout))
            return response

    def build_opener(*captured_handlers):
        handlers.extend(captured_handlers)
        return Opener()

    monkeypatch.setattr(urllib.request, "build_opener", build_opener)
    request = urllib.request.Request("https://example.com")

    assert open_no_redirect(request, 1.5) is response
    assert calls == [(request, 1.5)]
    proxy_handlers = [
        handler for handler in handlers if isinstance(handler, urllib.request.ProxyHandler)
    ]
    assert len(proxy_handlers) == 1
    assert proxy_handlers[0].proxies == {}
    assert any(isinstance(handler, _RejectRedirects) for handler in handlers)


def test_signed_request_redirects_are_rejected_without_forwarding_headers():
    request = urllib.request.Request(
        "https://abc.execute-api.us-east-1.amazonaws.com/v1/score-ais",
        headers={"Authorization": "secret-signature"},
        method="POST",
    )
    with pytest.raises(urllib.error.HTTPError, match="redirect refused"):
        _RejectRedirects().redirect_request(
            request,
            None,
            302,
            "Found",
            {},
            "https://attacker.example/steal",
        )


def _valid_score_response(mmsi: int = 200000001) -> bytes:
    return json.dumps(
        {
            "mmsi": mmsi,
            "score": 0.2,
            "confidence": 0.9,
            "reasons": [],
            "hitl_required": False,
            "trace_id": "trace-1",
            "latency_ms": 1.5,
            "n_history": 1,
        }
    ).encode()


def test_ais_score_response_requires_http_200_and_matching_schema():
    payload = validate_ais_score_response(200, _valid_score_response(), 200000001)
    assert payload["trace_id"] == "trace-1"
    with pytest.raises(ValueError, match="HTTP 200"):
        validate_ais_score_response(204, b"", 200000001)
    with pytest.raises(ValueError, match="valid JSON"):
        validate_ais_score_response(200, b"not-json", 200000001)
    with pytest.raises(ValueError, match="must equal"):
        validate_ais_score_response(200, _valid_score_response(200000002), 200000001)
    malformed = json.loads(_valid_score_response())
    malformed["reasons"] = ["not-a-score-reason"]
    with pytest.raises(ValueError, match="reason must be an object"):
        validate_ais_score_response(200, json.dumps(malformed).encode(), 200000001)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("score", 2, "score must be finite"),
        ("confidence", True, "confidence must be finite"),
        ("latency_ms", -1, "latency_ms must be finite"),
        ("n_history", -1, "n_history must be an integer"),
        ("reasons", "not-a-list", "reasons must be a list"),
        ("hitl_required", "false", "hitl_required must be a boolean"),
        ("trace_id", " ", "trace_id must be a non-empty string"),
    ),
)
def test_ais_score_response_rejects_invalid_top_level_fields(field, value, message):
    payload = json.loads(_valid_score_response())
    payload[field] = value

    with pytest.raises(ValueError, match=message):
        validate_ais_score_response(200, json.dumps(payload).encode(), 200000001)


def test_ais_score_response_requires_an_object():
    with pytest.raises(ValueError, match="must be a JSON object"):
        validate_ais_score_response(200, b"[]", 200000001)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("code", "", "code must be a non-empty string"),
        ("severity", 2, "severity must be within"),
        ("detail", None, "detail must be a string"),
        ("evidence", None, "evidence must be an object"),
    ),
)
def test_ais_score_response_rejects_invalid_reason_fields(field, value, message):
    payload = json.loads(_valid_score_response())
    reason = {
        "code": "OFF_CORRIDOR",
        "severity": 0.5,
        "detail": "outside corridor",
        "evidence": {},
    }
    reason[field] = value
    payload["reasons"] = [reason]

    with pytest.raises(ValueError, match=message):
        validate_ais_score_response(200, json.dumps(payload).encode(), 200000001)


def test_job_signs_inside_the_retried_send_callback():
    source = (Path(__file__).parents[1] / "job.py").read_text()
    send_block = source.split("def _send() -> None:", 1)[1].split("# Scoring is best-effort", 1)[0]
    assert send_block.index("sigv4_headers(") < send_block.index("open_no_redirect(")
    assert send_block.index("open_no_redirect(") < send_block.index("validate_ais_score_response(")


# --- Streaming robustness: bounded, non-blocking scorer retry ---------------


def test_post_scorer_success_first_try_no_retry_no_sleep():
    slept: list[float] = []
    errors: list[tuple[int, Exception]] = []
    calls = {"n": 0}

    def send():
        calls["n"] += 1

    ok, err = post_scorer_with_retry(
        send, sleep=slept.append, on_error=lambda a, e: errors.append((a, e))
    )
    assert ok is True
    assert err is None
    assert calls["n"] == 1  # no retry on the happy path
    assert slept == []  # and no backoff
    assert errors == []  # nothing logged


def test_post_scorer_retries_then_succeeds():
    # Fails once (transient), then succeeds: one logged error, one backoff, success.
    slept: list[float] = []
    errors: list[tuple[int, Exception]] = []
    seq = [RuntimeError("timeout"), None]

    def send():
        exc = seq.pop(0)
        if exc is not None:
            raise exc

    ok, err = post_scorer_with_retry(
        send,
        max_retries=2,
        sleep=slept.append,
        on_error=lambda a, e: errors.append((a, e)),
    )
    assert ok is True
    assert err is None
    assert len(errors) == 1  # the single failure was logged, not swallowed
    assert errors[0][0] == 0  # attempt index 0
    assert len(slept) == 1  # exactly one bounded backoff before the successful retry
    assert slept[0] > 0


def test_post_scorer_exhausts_retries_and_reports_failure_for_dlq():
    # Always fails: bounded attempts, every failure logged, then (False, last_error)
    # so the caller dead-letters. It must NOT raise into the Flink operator.
    slept: list[float] = []
    errors: list[tuple[int, Exception]] = []
    boom = ConnectionError("scorer down")

    def send():
        raise boom

    ok, err = post_scorer_with_retry(
        send,
        max_retries=2,
        sleep=slept.append,
        on_error=lambda a, e: errors.append((a, e)),
    )
    assert ok is False
    assert err is boom  # the last error is returned for the DLQ envelope
    assert len(errors) == 3  # 1 initial + 2 retries, each logged
    assert len(slept) == 2  # backoff only BETWEEN attempts, not after the last
    assert all(s >= 0 for s in slept)


def test_post_scorer_backoff_is_bounded_and_capped():
    # The default backoff grows exponentially but is capped, so retries stay fast and
    # a slow scorer cannot stall the operator (delays never exceed the cap).
    from flink.window_logic import SCORER_DELAY_CAP_S

    slept: list[float] = []

    def send():
        raise TimeoutError("slow")

    post_scorer_with_retry(send, max_retries=8, sleep=slept.append)
    assert len(slept) == 8
    assert all(0 <= s <= SCORER_DELAY_CAP_S for s in slept)
    # Non-decreasing then saturating at the cap (capped exponential).
    assert slept == sorted(slept)


def test_post_scorer_never_raises_even_without_error_callback():
    # No on_error provided: failures are still handled internally and the function
    # returns cleanly (the operator is never taken down by a scorer exception).
    def send():
        raise RuntimeError("boom")

    ok, err = post_scorer_with_retry(send, max_retries=1, sleep=lambda _s: None)
    assert ok is False
    assert isinstance(err, RuntimeError)


def test_post_scorer_negative_retries_makes_no_attempt():
    # Guard on the degenerate config max_retries < 0: the loop body never runs, so
    # send() is never called and the function returns a benign "no attempt" result
    # instead of raising or blocking.
    calls = {"n": 0}

    def send():
        calls["n"] += 1

    ok, err = post_scorer_with_retry(send, max_retries=-1, sleep=lambda _s: None)
    assert (ok, err) == (False, None)
    assert calls["n"] == 0
