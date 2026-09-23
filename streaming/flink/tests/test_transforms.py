"""Unit tests for the Flink job's pure transforms + window aggregation (gate G5)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from features.features import VESSEL_V_MAX_MPS, Fix, window_features
from flink.transforms import (
    P_PHYS_GATE,
    feature_item,
    parse_ais_json,
    passes_gate,
    score_request,
    window_representative,
)

T0 = datetime(2024, 6, 1, tzinfo=UTC)


def test_parse_ais_json_roundtrip():
    raw = json.dumps(
        {"mmsi": 200000001, "lat": 40.4, "lon": -74.0, "t": "2024-06-01T00:00:00Z", "sog": 9.7}
    )
    mmsi, fix = parse_ais_json(raw)
    assert mmsi == 200000001
    assert (fix.lat, fix.lon, fix.sog) == (40.4, -74.0, 9.7)
    assert fix.t == T0


def test_parse_ais_json_malformed_raises():
    with pytest.raises(ValueError):
        parse_ais_json('{"lat": 1.0}')  # missing mmsi


def test_window_representative_picks_latest():
    fixes = [
        Fix(lat=0, lon=0, t=T0 + timedelta(seconds=10)),
        Fix(lat=1, lon=1, t=T0 + timedelta(seconds=50)),
        Fix(lat=2, lon=2, t=T0 + timedelta(seconds=30)),
    ]
    rep = window_representative(fixes)
    assert rep.t == T0 + timedelta(seconds=50)


def test_window_representative_rejects_empty_window():
    with pytest.raises(ValueError, match="empty window"):
        window_representative([])


def test_p_physical_gate_matches_config_constant():
    # A reachable move (<= v_max) clears the gate; a wildly implausible one does not.
    reachable = window_features(
        Fix(lat=0.0, lon=0.0, t=T0 + timedelta(seconds=60)),
        Fix(lat=0.0, lon=0.0, t=T0),
    )
    assert reachable.p_physical == 1.0
    assert passes_gate(reachable, P_PHYS_GATE) is True

    # ~50 km in 60 s needs ~833 m/s, far over the 12.86 m/s cap -> p_physical tiny.
    jump = window_features(
        Fix(lat=0.0, lon=0.45, t=T0 + timedelta(seconds=60)),
        Fix(lat=0.0, lon=0.0, t=T0),
    )
    assert jump.p_physical < P_PHYS_GATE
    assert passes_gate(jump) is False


def test_feature_item_shape_and_ttl():
    feats = window_features(Fix(lat=0, lon=0, t=T0), None)
    item = feature_item(200000001, feats, T0, ttl_days=7)
    assert item["entity_id"] == "200000001"
    assert item["feature_name"] == "window"
    assert item["ttl"] == int(T0.timestamp()) + 7 * 86400
    assert item["p_physical"] == 1.0


def test_score_request_matches_serving_schema():
    fix = Fix(lat=40.4, lon=-74.0, t=T0, sog=9.7)
    req = score_request(200000001, fix)
    assert req["mmsi"] == 200000001
    assert req["fix"]["t"] == "2024-06-01T00:00:00Z"
    assert req["fix"]["lat"] == 40.4
    assert req["history"] == []


def test_score_request_includes_full_history():
    history = [
        Fix(lat=40.2, lon=-74.2, t=T0 - timedelta(minutes=3), sog=9.0),
        Fix(lat=40.3, lon=-74.1, t=T0 - timedelta(minutes=2), sog=9.0),
        Fix(lat=40.35, lon=-74.05, t=T0 - timedelta(minutes=1), sog=9.5),
    ]
    fix = Fix(lat=40.4, lon=-74.0, t=T0, sog=9.7)
    req = score_request(200000001, fix, history)
    assert len(req["history"]) == 3
    assert req["history"][0]["t"] == (T0 - timedelta(minutes=3)).isoformat().replace("+00:00", "Z")
    assert req["history"][-1]["t"] == (T0 - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")


def test_v_max_pinned_to_serving_config():
    # 25 kts * 0.514444 m/s/kt = 12.8611 m/s (guards against drift from serving).
    assert abs(VESSEL_V_MAX_MPS - 12.8611) < 1e-3
