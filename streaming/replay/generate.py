"""Deterministic generator for the synthetic AIS replay fixture (Phase 1.1).

Produces a few thousand AIS position reports over ~6 hours for a small fleet on
the demo sea-lane, with three planted, documented anomalies:

  - abnormal_gap     : a vessel goes silent for ~3 h then reappears at a plausible
                       location (signal-coverage denial).
  - implausible_jump : one fix requires ~6x the vessel speed cap (spoofing / GPS
                       error) but stays below the corrupt-data reject bound.
  - off_corridor     : a vessel runs ~10 km off the nearest sea-lane the whole time.

Every MMSI in the fleet starts with the MID 200. The ITU Table of Maritime
Identification Digits does not allocate that MID to any country, so no
administration can assign these numbers to a real ship.

The geometry is shared with the corridor artifact (serving/app/artifacts/corridors.json)
so the off-corridor case is consistent with the detector. Fully deterministic
(fixed seed, fixed start time) so the committed SHA256 is stable.

Run:  python -m replay.generate    (writes the fixture, expectations.json, sha256)
"""

from __future__ import annotations

import hashlib
import json
import math
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

R = 6_371_000.0
KNOTS = 0.514_444
SEED = 7
START = datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC)
STEP_S = 60
N_STEPS = 360  # 6 hours of 1-minute fixes (indices 0..360)

REPO = Path(__file__).resolve().parents[2]
CORRIDORS = REPO / "serving" / "app" / "artifacts" / "corridors.json"
FIX_DIR = REPO / "streaming" / "fixtures"
JSONL = FIX_DIR / "ais_recorded.jsonl"
EXPECT = FIX_DIR / "expectations.json"
SHA = FIX_DIR / "ais_recorded.sha256"


def _lane_lonlat() -> list[tuple[float, float]]:
    data = json.loads(CORRIDORS.read_text())
    return [tuple(p) for p in data["lanes"][0]["nodes"]]


# --- local equirectangular projection around the lane centroid --------------


def _centroid(lane: list[tuple[float, float]]) -> tuple[float, float]:
    lon = sum(p[0] for p in lane) / len(lane)
    lat = sum(p[1] for p in lane) / len(lane)
    return lat, lon


class _Proj:
    def __init__(self, lat0: float, lon0: float) -> None:
        self.lat0 = lat0
        self.lon0 = lon0
        self.coslat = math.cos(math.radians(lat0))

    def to_xy(self, lat: float, lon: float) -> tuple[float, float]:
        x = math.radians(lon - self.lon0) * self.coslat * R
        y = math.radians(lat - self.lat0) * R
        return x, y

    def to_ll(self, x: float, y: float) -> tuple[float, float]:
        lat = self.lat0 + math.degrees(y / R)
        lon = self.lon0 + math.degrees(x / (R * self.coslat))
        return lat, lon


@dataclass
class _Lane:
    pts: list[tuple[float, float]]  # xy in meters
    cum: list[float]  # cumulative arc length

    @property
    def length(self) -> float:
        return self.cum[-1]

    def at(self, d: float) -> tuple[float, float, float, float]:
        """Return (x, y, ux, uy): position at arc-length d and unit tangent."""

        d = max(0.0, min(d, self.length))
        for i in range(len(self.pts) - 1):
            if d <= self.cum[i + 1] or i == len(self.pts) - 2:
                (x0, y0), (x1, y1) = self.pts[i], self.pts[i + 1]
                seg = self.cum[i + 1] - self.cum[i]
                f = 0.0 if seg == 0 else (d - self.cum[i]) / seg
                ux, uy = (x1 - x0), (y1 - y0)
                n = math.hypot(ux, uy) or 1.0
                ux, uy = ux / n, uy / n
                return x0 + (x1 - x0) * f, y0 + (y1 - y0) * f, ux, uy
        raise AssertionError


def _build_lane(proj: _Proj, lane_lonlat: list[tuple[float, float]]) -> _Lane:
    pts = [proj.to_xy(lat, lon) for lon, lat in lane_lonlat]
    cum = [0.0]
    for i in range(1, len(pts)):
        cum.append(cum[-1] + math.hypot(pts[i][0] - pts[i - 1][0], pts[i][1] - pts[i - 1][1]))
    return _Lane(pts, cum)


def _bearing_deg(ux: float, uy: float) -> float:
    # x=east, y=north; bearing measured clockwise from north
    return math.degrees(math.atan2(ux, uy)) % 360.0


def _emit(proj: _Proj, mmsi: int, x: float, y: float, t: datetime, sog_mps: float, brg: float):
    lat, lon = proj.to_ll(x, y)
    return {
        "mmsi": mmsi,
        "lat": round(lat, 6),
        "lon": round(lon, 6),
        "t": t.isoformat().replace("+00:00", "Z"),
        "sog": round(sog_mps / KNOTS, 1),
        "cog": round(brg, 1),
        "heading": round(brg, 1),
    }


def generate() -> dict:
    rng = random.Random(SEED)  # nosec B311  # deterministic replay fixture generation, not security-sensitive
    lane_lonlat = _lane_lonlat()
    lat0, lon0 = _centroid(lane_lonlat)
    proj = _Proj(lat0, lon0)
    lane = _build_lane(proj, lane_lonlat)

    records: list[dict] = []
    anomalies: list[dict] = []
    normal_samples: list[dict] = []

    def lateral(d: float, off_m: float) -> tuple[float, float, float]:
        x, y, ux, uy = lane.at(d)
        nx, ny = -uy, ux  # left normal
        return x + nx * off_m, y + ny * off_m, _bearing_deg(ux, uy)

    # --- 5 normal vessels: constant speed, small fixed lateral offset --------
    sample_i = 180  # the mid-track fix used as the documented normal sample
    for k in range(5):
        mmsi = 200_000_010 + k
        v = 4.0 + 0.5 * k  # 4.0 .. 6.0 m/s (~8-12 kts)
        s0 = 3_000.0 + 9_000.0 * k  # staggered starts along the lane
        off = rng.uniform(-250.0, 250.0)
        sample_t = None
        for i in range(N_STEPS + 1):
            t = START + timedelta(seconds=i * STEP_S)
            wobble = 50.0 * math.sin(i / 30.0)  # mild, low-derivative
            x, y, brg = lateral(s0 + v * i * STEP_S, off + wobble)
            rec = _emit(proj, mmsi, x, y, t, v, brg)
            records.append(rec)
            if i == sample_i:
                sample_t = rec["t"]
        normal_samples.append({"mmsi": mmsi, "t": sample_t})

    # --- abnormal_gap vessel: 3 h silence then plausible reappearance --------
    mmsi_gap = 200_000_001
    v = 5.0
    s0 = 1_000.0
    gap_start_i, gap_end_i = 120, 300  # silent for minutes (120, 300) -> 180 min gap
    reappear_t = None
    for i in range(N_STEPS + 1):
        if gap_start_i < i < gap_end_i:
            continue
        t = START + timedelta(seconds=i * STEP_S)
        x, y, brg = lateral(s0 + v * i * STEP_S, 0.0)
        records.append(_emit(proj, mmsi_gap, x, y, t, v, brg))
        if i == gap_end_i:
            reappear_t = t.isoformat().replace("+00:00", "Z")
    anomalies.append({
        "mmsi": mmsi_gap,
        "kind": "abnormal_gap",
        "t": reappear_t,
        "expect_reason": "abnormal_gap",
        "expect_hitl": True,
        "note": "180-minute coverage gap; vessel reappears at a kinematically reachable point.",
    })

    # --- implausible_jump vessel: one fix at ~6x v_max (under corrupt bound) --
    mmsi_jump = 200_000_002
    v = 5.0
    s0 = 2_000.0
    jump_i = 200
    jump_t = None
    for i in range(N_STEPS + 1):
        t = START + timedelta(seconds=i * STEP_S)
        # one ~5 km jump ALONG the lane at jump_i (single implausible-speed segment,
        # stays on-corridor with no spurious heading change), then continue normally.
        extra = 5_000.0 if i >= jump_i else 0.0
        x, y, brg = lateral(s0 + v * i * STEP_S + extra, 0.0)
        records.append(_emit(proj, mmsi_jump, x, y, t, v, brg))
        if i == jump_i:
            jump_t = t.isoformat().replace("+00:00", "Z")
    anomalies.append({
        "mmsi": mmsi_jump,
        "kind": "implausible_jump",
        "t": jump_t,
        "expect_reason": "implausible_speed",
        "expect_hitl": True,
        "note": "One fix jumps ~5 km along the lane in 60 s (~6.9x the vessel speed cap); "
                "on-corridor and below the corrupt-data reject bound, so scored not rejected.",
    })

    # --- off_corridor vessel: ~10 km off the lane the whole track ------------
    mmsi_off = 200_000_003
    v = 5.0
    s0 = 6_000.0
    off_sample_i = 200
    off_t = None
    for i in range(N_STEPS + 1):
        t = START + timedelta(seconds=i * STEP_S)
        x, y, brg = lateral(s0 + v * i * STEP_S, -10_000.0)  # 10 km off the lane
        records.append(_emit(proj, mmsi_off, x, y, t, v, brg))
        if i == off_sample_i:
            off_t = t.isoformat().replace("+00:00", "Z")
    anomalies.append({
        "mmsi": mmsi_off,
        "kind": "off_corridor",
        "t": off_t,
        "expect_reason": "off_corridor",
        "expect_hitl": True,
        "note": "Vessel operates ~10 km off the nearest sea-lane at a plausible speed.",
    })

    # stable order: by (mmsi, t)
    records.sort(key=lambda r: (r["mmsi"], r["t"]))

    FIX_DIR.mkdir(parents=True, exist_ok=True)
    body = "\n".join(json.dumps(r, separators=(",", ":"), sort_keys=True) for r in records) + "\n"
    JSONL.write_text(body)
    sha = hashlib.sha256(body.encode()).hexdigest()
    SHA.write_text(sha + "\n")

    expectations = {
        "fixture": "ais_recorded.jsonl",
        "sha256": sha,
        "n_records": len(records),
        "window_hours": (N_STEPS * STEP_S) / 3600.0,
        "generated_from": "replay.generate (seed=7, start=2024-06-01T00:00:00Z)",
        "provenance": (
            "The vessels, MMSIs, positions, and anomalies are fictional and were generated "
            "for testing. Every MMSI starts with the MID 200. The ITU Table of Maritime "
            "Identification Digits does not allocate MID 200 to any country, so no "
            "administration can assign these numbers to a real ship."
        ),
        "anomalies": anomalies,
        "normal_samples": normal_samples,
    }
    EXPECT.write_text(json.dumps(expectations, indent=2) + "\n")
    return expectations


if __name__ == "__main__":
    exp = generate()
    print(f"wrote {exp['n_records']} records to {JSONL.relative_to(REPO)}")
    print(f"sha256 {exp['sha256']}")
    for a in exp["anomalies"]:
        print(f"  anomaly {a['mmsi']} {a['kind']} @ {a['t']} -> {a['expect_reason']}")
