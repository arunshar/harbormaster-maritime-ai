"""Static sea-lane corridor graph + lane association geometry.

Loads the frozen corridor artifact (a sea-lane graph) read-only and
answers two geometric questions used by the CorridorDeviationDetector:

  - the perpendicular distance from a fix to the nearest sea-lane edge, and
  - the distance from a fix to the nearest expected course-change waypoint.

Math is done in a local equirectangular projection centered on the lane centroid,
so distances are Euclidean to first order over the demo region.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

R = 6_371_000.0

# Resolve the default artifact relative to the serving package root (serving/).
_PKG_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class _Proj:
    lat0: float
    lon0: float
    coslat: float

    def to_xy(self, lat: float, lon: float) -> tuple[float, float]:
        x = math.radians(lon - self.lon0) * self.coslat * R
        y = math.radians(lat - self.lat0) * R
        return x, y


def _pt_seg_dist(px, py, ax, ay, bx, by) -> float:
    dx, dy = bx - ax, by - ay
    if dx == 0.0 and dy == 0.0:
        return math.hypot(px - ax, py - ay)
    t = ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)
    t = max(0.0, min(1.0, t))
    cx, cy = ax + t * dx, ay + t * dy
    return math.hypot(px - cx, py - cy)


@dataclass
class CorridorGraph:
    proj: _Proj
    # lanes: list of (lane_id, [ (x,y), ... ]) polylines in projected meters
    lanes: list[tuple[str, list[tuple[float, float]]]]
    waypoints_xy: list[tuple[float, float]]

    def nearest_lane_distance_m(self, lat: float, lon: float) -> tuple[float, str]:
        px, py = self.proj.to_xy(lat, lon)
        best = math.inf
        best_id = ""
        for lane_id, pts in self.lanes:
            for (ax, ay), (bx, by) in zip(pts, pts[1:], strict=False):
                d = _pt_seg_dist(px, py, ax, ay, bx, by)
                if d < best:
                    best, best_id = d, lane_id
        return best, best_id

    def nearest_waypoint_m(self, lat: float, lon: float) -> float:
        px, py = self.proj.to_xy(lat, lon)
        if not self.waypoints_xy:
            return math.inf
        return min(math.hypot(px - wx, py - wy) for wx, wy in self.waypoints_xy)


def load_corridors(path: str | Path | None = None) -> CorridorGraph:
    p = Path(path) if path else _PKG_ROOT / "app" / "artifacts" / "corridors.json"
    if not p.is_absolute() and not p.exists():
        # config stores a package-relative path like "app/artifacts/corridors.json"
        p = _PKG_ROOT / p
    data = json.loads(p.read_text())

    all_lonlat: list[tuple[float, float]] = []
    for lane in data["lanes"]:
        all_lonlat.extend(tuple(n) for n in lane["nodes"])
    lon0 = sum(c[0] for c in all_lonlat) / len(all_lonlat)
    lat0 = sum(c[1] for c in all_lonlat) / len(all_lonlat)
    proj = _Proj(lat0=lat0, lon0=lon0, coslat=math.cos(math.radians(lat0)))

    lanes: list[tuple[str, list[tuple[float, float]]]] = []
    for lane in data["lanes"]:
        pts = [proj.to_xy(lat, lon) for lon, lat in lane["nodes"]]
        lanes.append((lane["id"], pts))
    waypoints_xy = [proj.to_xy(w["lonlat"][1], w["lonlat"][0]) for w in data.get("waypoints", [])]
    return CorridorGraph(proj=proj, lanes=lanes, waypoints_xy=waypoints_xy)
