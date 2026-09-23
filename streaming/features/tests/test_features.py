"""Unit tests for the streaming feature library (Phase 1.5 gate G5, local part)."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

import pytest

from features import (
    KNOTS_TO_MPS,
    VESSEL_V_MAX_KTS,
    VESSEL_V_MAX_MPS,
    Fix,
    gap_since_last_s,
    haversine_m,
    p_physical,
    v_required_mps,
    window_features,
)

T0 = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)


def test_v_max_constant_is_25_kts_exactly():
    assert VESSEL_V_MAX_KTS == 25.0
    assert VESSEL_V_MAX_MPS == 25.0 * KNOTS_TO_MPS
    assert math.isclose(VESSEL_V_MAX_MPS, 12.8611, abs_tol=1e-4)


def test_feature_v_max_matches_serving_config_exactly():
    """The Flink feature v_max must equal the serving config's vessel v_max."""

    from app.config import Settings

    assert VESSEL_V_MAX_MPS == Settings().vessel_v_max_mps


def test_p_physical_equals_the_config_constant_formula():
    # at exactly v_max, plausibility saturates to 1.0
    assert p_physical(VESSEL_V_MAX_MPS) == 1.0
    # below v_max, still 1.0 (reachable)
    assert p_physical(0.5 * VESSEL_V_MAX_MPS) == 1.0
    # at 2x v_max, exactly 0.5; at 4x, exactly 0.25
    assert p_physical(2.0 * VESSEL_V_MAX_MPS) == pytest.approx(0.5)
    assert p_physical(4.0 * VESSEL_V_MAX_MPS) == pytest.approx(0.25)
    # explicit v_max argument is honored
    assert p_physical(10.0, v_max_mps=10.0) == 1.0
    assert p_physical(20.0, v_max_mps=10.0) == pytest.approx(0.5)


def test_haversine_one_degree_latitude():
    # 1 degree of latitude is ~111.2 km on a sphere of radius 6_371_000 m
    d = haversine_m(40.0, -73.0, 41.0, -73.0)
    assert math.isclose(d, math.radians(1.0) * 6_371_000.0, rel_tol=1e-9)
    assert haversine_m(40.0, -73.0, 40.0, -73.0) == 0.0


def test_v_required_and_gap():
    assert v_required_mps(1000.0, 100.0) == 10.0
    assert gap_since_last_s(T0, T0 + timedelta(minutes=5)) == 300.0
    assert gap_since_last_s(T0 + timedelta(minutes=5), T0) == 0.0  # clamped at 0


def test_window_features_first_fix_is_benign():
    wf = window_features(Fix(40.5, -73.9, T0, sog=10.0, cog=90.0, heading=88.0), None)
    assert wf.gap_since_last_s == 0.0
    assert wf.distance_m == 0.0
    assert wf.v_required_mps == 0.0
    assert wf.p_physical == 1.0
    assert wf.sog == 10.0


def test_window_features_plausible_and_implausible_moves():
    prev = Fix(40.50, -73.90, T0)
    # ~0.01 deg lat in 1 min -> ~1112 m / 60 s ~ 18.5 m/s > v_max -> p_physical < 1
    fast = window_features(Fix(40.51, -73.90, T0 + timedelta(minutes=1)), prev)
    assert fast.distance_m == pytest.approx(haversine_m(40.50, -73.90, 40.51, -73.90))
    assert fast.v_required_mps > VESSEL_V_MAX_MPS
    assert fast.p_physical < 1.0
    # same move over 1 hour is easily reachable -> p_physical == 1.0
    slow = window_features(Fix(40.51, -73.90, T0 + timedelta(hours=1)), prev)
    assert slow.p_physical == 1.0
