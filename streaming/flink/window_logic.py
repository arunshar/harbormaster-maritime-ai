"""Pure, pyflink-free window logic for the Managed Flink feature job (gate G5).

These are the feature/transform helpers the Flink job runs per vessel: compute a
window's features against the previous fix, apply the P_phys cheap-gate, shape the
DynamoDB feature item, and build the /v1/score-ais request body. They import NO
pyflink, so they unit-test without a Flink runtime or AWS.

This block is the same byte-identical duplicate that job.py used to inline (job.py
now imports these names from here instead). It is still a deliberate duplicate of
the tested source of truth in streaming.features.features and
streaming.flink.transforms; the copy exists because FeatureProcess.process_element's
stateful KeyedProcessFunction runs in a separate Python UDF worker subprocess
(Beam's process-mode harness) that does not inherit the driver's sys.path, and
cloudpickle only serializes a referenced function/class BY VALUE when it is defined
in __main__. Keeping these in a sibling module job.py imports from lets them be
imported and tested directly (pyflink absent) while job.py re-inlines them into
__main__ at runtime by importing them at module top; see job.py's module docstring
for the full dependency-staging war story.
"""

from __future__ import annotations

import json
import math
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache

# --- inlined from streaming/features/features.py (kept byte-identical; see the
# module docstring above for why this is a duplicate, not an import) ---

EARTH_RADIUS_M = 6_371_000.0
KNOTS_TO_MPS = 0.514_444

# Pinned to the serving config (Settings.vessel_v_max_kts). 25 kts = 12.8611 m/s.
VESSEL_V_MAX_KTS = 25.0
VESSEL_V_MAX_MPS = VESSEL_V_MAX_KTS * KNOTS_TO_MPS

_EPS = 1e-6


@dataclass(frozen=True)
class Fix:
    """One AIS position report (the Flink job's input element)."""

    lat: float
    lon: float
    t: datetime
    sog: float | None = None  # knots
    cog: float | None = None  # deg
    heading: float | None = None  # deg


@dataclass(frozen=True)
class WindowFeatures:
    """Features emitted for one vessel's 1-minute window."""

    sog: float | None
    cog: float | None
    heading: float | None
    gap_since_last_s: float
    distance_m: float
    v_required_mps: float
    p_physical: float


def haversine_m(a_lat: float, a_lon: float, b_lat: float, b_lon: float) -> float:
    """Great-circle distance in meters."""

    phi1, phi2 = math.radians(a_lat), math.radians(b_lat)
    dphi = phi2 - phi1
    dlam = math.radians(b_lon - a_lon)
    s = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(min(1.0, math.sqrt(s)))


def gap_since_last_s(t_prev: datetime, t_now: datetime) -> float:
    """Seconds since the previous fix (>= 0)."""

    return max(0.0, (t_now - t_prev).total_seconds())


def v_required_mps(distance_m: float, dt_s: float) -> float:
    """Minimum speed (m/s) needed to cover distance_m in dt_s."""

    return distance_m / max(dt_s, _EPS)


def p_physical(v_req_mps: float, v_max_mps: float = VESSEL_V_MAX_MPS) -> float:
    """Kinematic plausibility in (0, 1]: min(1, v_max / max(v_required, eps)).

    1.0 when the move is reachable at v_max; < 1 when it requires more than v_max.
    """

    return min(1.0, v_max_mps / max(v_req_mps, _EPS))


def window_features(
    curr: Fix,
    prev: Fix | None,
    *,
    v_max_mps: float = VESSEL_V_MAX_MPS,
) -> WindowFeatures:
    """Compute the window's features from the current fix and the previous one.

    With no previous fix (vessel's first window) the inter-fix features are zero
    and p_physical is 1.0 (nothing to contradict).
    """

    if prev is None:
        return WindowFeatures(
            sog=curr.sog,
            cog=curr.cog,
            heading=curr.heading,
            gap_since_last_s=0.0,
            distance_m=0.0,
            v_required_mps=0.0,
            p_physical=1.0,
        )
    dt_s = gap_since_last_s(prev.t, curr.t)
    dist = haversine_m(prev.lat, prev.lon, curr.lat, curr.lon)
    v_req = v_required_mps(dist, dt_s)
    return WindowFeatures(
        sog=curr.sog,
        cog=curr.cog,
        heading=curr.heading,
        gap_since_last_s=dt_s,
        distance_m=dist,
        v_required_mps=v_req,
        p_physical=p_physical(v_req, v_max_mps),
    )


# --- inlined from streaming/flink/transforms.py (kept byte-identical; see the
# module docstring above for why this is a duplicate, not an import) ---

# The P_phys cheap-gate: at or above this the event is scored; below it the event
# is dropped / low-priority and never reaches the serving scorer (plan's 0.3).
P_PHYS_GATE = 0.3


def parse_ais_json(raw: str | bytes) -> tuple[int, Fix]:
    """Parse one ais-raw Kinesis record into (mmsi, Fix).

    Raises ValueError when JSON decoding, required-field conversion, or MMSI
    validation fails so the job can route the record to a dead-letter path.
    """
    try:
        d = json.loads(raw)
        raw_mmsi = d["mmsi"]
        if type(raw_mmsi) is int:
            mmsi = raw_mmsi
        elif isinstance(raw_mmsi, str) and raw_mmsi.isascii() and raw_mmsi.isdecimal():
            mmsi = int(raw_mmsi)
        else:
            raise TypeError("mmsi must be an integer or ASCII digit string")
        if not 0 <= mmsi <= 999_999_999:
            raise ValueError(f"mmsi out of range: {mmsi}")
        return mmsi, Fix(
            lat=float(d["lat"]),
            lon=float(d["lon"]),
            t=datetime.fromisoformat(str(d["t"]).replace("Z", "+00:00")),
            sog=None if d.get("sog") is None else float(d["sog"]),
            cog=None if d.get("cog") is None else float(d["cog"]),
            heading=None if d.get("heading") is None else float(d["heading"]),
        )
    except (KeyError, OverflowError, RecursionError, ValueError, TypeError) as exc:
        raise ValueError(f"malformed ais record: {exc}") from exc


INVALID_AIS_KEY = -1


def mmsi_partition_key(raw: str | bytes) -> int:
    """Return a LONG-compatible MMSI key without failing before quarantine.

    Flink evaluates key_by before the keyed process function. Records rejected
    by this parser share a sentinel partition so FeatureProcess can quarantine
    them; they return before reading or updating keyed state.
    """
    try:
        mmsi, _ = parse_ais_json(raw)
        return mmsi
    except ValueError:
        return INVALID_AIS_KEY


def passes_gate(feats: WindowFeatures, threshold: float = P_PHYS_GATE) -> bool:
    """True if the window's p_physical clears the cheap-gate (send to the scorer)."""
    return feats.p_physical >= threshold


def feature_item(mmsi: int, feats: WindowFeatures, ts: datetime, ttl_days: int = 7) -> dict:
    """A DynamoDB item for the Feast online table (entity_id + feature_name keyed)."""
    return {
        "entity_id": str(mmsi),
        "feature_name": "window",
        "t": ts.isoformat().replace("+00:00", "Z"),
        "gap_since_last_s": feats.gap_since_last_s,
        "distance_m": feats.distance_m,
        "v_required_mps": feats.v_required_mps,
        "p_physical": feats.p_physical,
        "sog": feats.sog,
        "cog": feats.cog,
        "heading": feats.heading,
        "ttl": int(ts.timestamp()) + ttl_days * 86400,
    }


def _fix_dict(f: Fix) -> dict:
    return {
        "lat": f.lat,
        "lon": f.lon,
        "t": f.t.isoformat().replace("+00:00", "Z"),
        "sog": f.sog,
        "cog": f.cog,
        "heading": f.heading,
    }


def score_request(mmsi: int, fix: Fix, history: list[Fix] | None = None) -> dict:
    """The POST /v1/score-ais body for one gated event, matching serving's
    AisScoreIn schema: {mmsi, fix, history}. The scorer recomputes its own
    anomaly features server-side from fix + history; it has no features field
    (an earlier version of this function sent Flink's own precomputed
    WindowFeatures under "features", which the real schema never had -- every
    scorer call 422'd, a real first-live-run finding, W1 sprint window,
    2026-07-04). history is Flink's own keyed state: the vessel's recent prior
    fixes, oldest first. serving's HeuristicPlanner only routes to abnormal-gap
    detection when n_history (len(history)) is >= 3 (app/agents/
    heuristic_planner.py); sending fewer means gap anomalies are silently never
    evaluated, not just under-scored -- also a real first-live-run finding, W1
    sprint window, 2026-07-04."""
    return {
        "mmsi": mmsi,
        "fix": _fix_dict(fix),
        "history": [_fix_dict(f) for f in (history or [])],
    }


# --- Streaming robustness: quarantine (DLQ) + bounded scorer retry -----------
# Pure, pyflink-free so they unit-test without a Flink runtime or AWS. job.py
# wires them to a real S3 quarantine sink and the urllib scorer POST.

# Scorer POST retry policy: a few bounded attempts, then dead-letter. Deliberately
# small so a slow/unhealthy scorer cannot stall a Flink task (retries run inline in
# process_element; the scorer POST is best-effort, HITL still catches misses later).
SCORER_MAX_RETRIES = 2
SCORER_BASE_DELAY_S = 0.2
SCORER_DELAY_CAP_S = 2.0


@lru_cache(maxsize=1)
def _runtime_botocore_session():
    """Reuse credential-provider state while retaining refreshable credentials."""
    from botocore.session import get_session

    return get_session()


def validate_execute_api_url(url: str, region: str) -> str:
    """Require the exact regional API Gateway HTTPS scoring route."""
    parsed = urllib.parse.urlsplit(url)
    host = parsed.hostname or ""
    dns_suffix = "amazonaws.com.cn" if region.startswith("cn-") else "amazonaws.com"
    expected_host = re.compile(
        rf"^[a-z0-9]+\.execute-api\.{re.escape(region)}\.{re.escape(dns_suffix)}$"
    )
    if (
        parsed.scheme != "https"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port is not None
        or not expected_host.fullmatch(host)
        or parsed.path != "/v1/score-ais"
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            "scorer URL must be the exact regional HTTPS execute-api /v1/score-ais route"
        )
    return url


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    """Never forward SigV4 headers to a redirect destination."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(
            req.full_url,
            code,
            "redirect refused for signed request",
            headers,
            fp,
        )


def open_no_redirect(request: urllib.request.Request, timeout_seconds: float = 5):
    """Open one signed request while refusing every HTTP redirect."""
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be finite and > 0")
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        _RejectRedirects(),
    )
    return opener.open(request, timeout=timeout_seconds)  # nosec B310


def validate_ais_score_response(
    status_code: int,
    body: bytes,
    expected_mmsi: int,
) -> dict:
    """Validate the response contract before a scorer call counts as success."""
    if status_code != 200:
        raise ValueError(f"expected scorer HTTP 200, got {status_code}")
    try:
        payload = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("scorer response is not valid JSON") from error
    if not isinstance(payload, dict):
        raise ValueError("scorer response must be a JSON object")

    mmsi = payload.get("mmsi")
    if isinstance(mmsi, bool) or not isinstance(mmsi, int) or mmsi != expected_mmsi:
        raise ValueError(f"scorer response mmsi must equal {expected_mmsi}")

    for name in ("score", "confidence"):
        value = payload.get(name)
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or not 0 <= value <= 1
        ):
            raise ValueError(f"scorer response {name} must be finite and within [0, 1]")

    latency_ms = payload.get("latency_ms")
    if (
        isinstance(latency_ms, bool)
        or not isinstance(latency_ms, (int, float))
        or not math.isfinite(latency_ms)
        or latency_ms < 0
    ):
        raise ValueError("scorer response latency_ms must be finite and >= 0")
    n_history = payload.get("n_history")
    if isinstance(n_history, bool) or not isinstance(n_history, int) or n_history < 0:
        raise ValueError("scorer response n_history must be an integer >= 0")
    reasons = payload.get("reasons")
    if not isinstance(reasons, list):
        raise ValueError("scorer response reasons must be a list")
    for reason in reasons:
        if not isinstance(reason, dict):
            raise ValueError("each scorer response reason must be an object")
        if not isinstance(reason.get("code"), str) or not reason["code"]:
            raise ValueError("scorer response reason code must be a non-empty string")
        severity = reason.get("severity")
        if (
            isinstance(severity, bool)
            or not isinstance(severity, (int, float))
            or not math.isfinite(severity)
            or not 0 <= severity <= 1
        ):
            raise ValueError("scorer response reason severity must be within [0, 1]")
        if not isinstance(reason.get("detail"), str):
            raise ValueError("scorer response reason detail must be a string")
        if not isinstance(reason.get("evidence"), dict):
            raise ValueError("scorer response reason evidence must be an object")
    if not isinstance(payload.get("hitl_required"), bool):
        raise ValueError("scorer response hitl_required must be a boolean")
    trace_id = payload.get("trace_id")
    if not isinstance(trace_id, str) or not trace_id.strip():
        raise ValueError("scorer response trace_id must be a non-empty string")
    return payload


def sigv4_headers(url: str, body: bytes, region: str) -> dict[str, str]:
    """Create fresh API Gateway SigV4 headers from the runtime role.

    The provider session is cached, but get_credentials and
    get_frozen_credentials run for every request so refreshable Managed Flink
    credentials and retry timestamps never go stale. boto3 ships botocore into
    the UDF worker through requirements.txt.
    """
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest

    validate_execute_api_url(url, region)
    credentials = _runtime_botocore_session().get_credentials()
    if credentials is None:
        raise RuntimeError("no AWS credentials available for the signed serving request")
    request = AWSRequest(
        method="POST",
        url=url,
        data=body,
        headers={"Content-Type": "application/json"},
    )
    SigV4Auth(credentials.get_frozen_credentials(), "execute-api", region).add_auth(request)
    return {str(key): str(value) for key, value in request.headers.items()}


def quarantine_envelope(raw: str | bytes, reason: str, now: datetime) -> dict:
    """Dead-letter record for a message the job could not process.

    Wraps the offending payload with the failure reason and an ingest timestamp so a
    quarantined record is self-describing when replayed or triaged. `raw` is stored
    as text (decoded best-effort) rather than dropped, which is the whole point of a
    DLQ over the previous silent return.
    """
    if isinstance(raw, bytes):
        payload = raw.decode("utf-8", errors="replace")
    else:
        payload = raw
    return {
        "reason": reason,
        "raw": payload,
        "quarantined_at": now.isoformat().replace("+00:00", "Z"),
    }


def _scorer_backoff(attempt: int) -> float:
    """Capped exponential backoff (seconds) for the scorer retry; attempt is 0-based."""
    return min(SCORER_DELAY_CAP_S, SCORER_BASE_DELAY_S * (2 ** max(0, attempt)))


def post_scorer_with_retry(
    send: Callable[[], None],
    *,
    max_retries: int = SCORER_MAX_RETRIES,
    sleep: Callable[[float], None] = time.sleep,
    on_error: Callable[[int, Exception], None] | None = None,
    backoff: Callable[[int], float] = _scorer_backoff,
) -> tuple[bool, Exception | None]:
    """Call `send` (the scorer POST), retrying a bounded number of times on failure.

    Returns (ok, last_error). ok is True on the first success. On every failure
    on_error(attempt, exc) is invoked (job.py logs it) so a failure is never silently
    swallowed the way the old bare `except: pass` did. Between attempts it sleeps a
    capped backoff; after max_retries it gives up and returns (False, last_error) so
    the caller can dead-letter the request. It never raises: a scorer outage must not
    take down the Flink operator. `send`, `sleep`, `on_error`, and `backoff` are all
    injected so this is fully unit-testable with no network and no real waiting.
    """
    last_error: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            send()
            return True, None
        except Exception as exc:  # noqa: BLE001  # bounded here; caller dead-letters
            last_error = exc
            if on_error is not None:
                on_error(attempt, exc)
            if attempt >= max_retries:
                return False, last_error
            sleep(backoff(attempt))
    return False, last_error
