"""Per-vessel streaming feature functions (Flink feature job, Phase 1.5).

These are the exact features the Flink per-vessel keyed process function
computes per MMSI once per fix, against the previous fix (ADR 0001). They are
written as pure functions so they test without a Flink runtime. The kinematic
constant is pinned to 25 kts (the vessel v_max) to
match the serving config exactly; `p_physical` is the cheap gate the Flink job
applies before calling the serving scorer.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime

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
