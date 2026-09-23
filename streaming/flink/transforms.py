"""Pure, runtime-free transforms for the Flink feature job (gate G5).

The Flink per-vessel keyed process function (job.py) computes features once per
fix, against the previous fix (ADR 0001), via streaming.features.window_features.
It then uses these helpers to gate, shape the DynamoDB feature item, and build the
/v1/score-ais request. Kept pure so they unit-test without a Flink runtime or AWS.
"""

from __future__ import annotations

import json
from datetime import datetime

from features.features import Fix, WindowFeatures

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


def window_representative(fixes: list[Fix]) -> Fix:
    """The 1-min tumbling window reduces to the vessel's latest fix in the window."""
    if not fixes:
        raise ValueError("empty window")
    return max(fixes, key=lambda f: f.t)


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
