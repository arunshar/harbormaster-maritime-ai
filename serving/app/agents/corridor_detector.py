"""CorridorDeviationDetector. Third deterministic agent (no LLM).

Runs the lane association test against the static sea-lane graph and emits two
signals into the same fusion + HITL path as the gap and speed detectors:

  - off_corridor   : the current fix is farther than off_corridor_threshold_m
                     from the nearest sea-lane edge (positional deviation).
  - unexpected_node: a large course change occurs far from any expected
                     waypoint node.

Grounding and design in docs/corridor-detector.md. Inline CPU, no always-on
cost; the corridor graph is loaded read-only once at startup.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from app.components.corridor import CorridorGraph, load_corridors
from app.config import Settings
from app.models import Anchor


@dataclass(frozen=True)
class CorridorFinding:
    off_corridor: bool
    distance_m: float
    nearest_lane: str
    unexpected_node: bool
    heading_change_deg: float
    nearest_waypoint_m: float


def _bearing_deg(a: Anchor, b: Anchor) -> float:
    # course over ground from a to b, clockwise from north
    dlon = math.radians(b.lon - a.lon)
    y = math.sin(dlon) * math.cos(math.radians(b.lat))
    x = math.cos(math.radians(a.lat)) * math.sin(math.radians(b.lat)) - math.sin(
        math.radians(a.lat)
    ) * math.cos(math.radians(b.lat)) * math.cos(dlon)
    return math.degrees(math.atan2(y, x)) % 360.0


def _ang_diff(b1: float, b2: float) -> float:
    d = abs(b1 - b2) % 360.0
    return d if d <= 180.0 else 360.0 - d


class CorridorDeviationDetector:
    def __init__(self, settings: Settings, graph: CorridorGraph | None = None) -> None:
        self.settings = settings
        self.graph = graph or load_corridors(settings.corridor_artifact_path)

    async def detect(self, track: list[Anchor]) -> CorridorFinding:
        cur = track[-1]
        dist, lane_id = self.graph.nearest_lane_distance_m(cur.lat, cur.lon)
        off = dist > self.settings.off_corridor_threshold_m

        heading_change = 0.0
        nearest_wp = self.graph.nearest_waypoint_m(cur.lat, cur.lon)
        unexpected = False
        if len(track) >= 3:
            b1 = _bearing_deg(track[-3], track[-2])
            b2 = _bearing_deg(track[-2], track[-1])
            heading_change = _ang_diff(b1, b2)
            unexpected = (
                heading_change > self.settings.unexpected_node_heading_deg
                and nearest_wp > self.settings.waypoint_radius_m
            )

        return CorridorFinding(
            off_corridor=off,
            distance_m=dist,
            nearest_lane=lane_id,
            unexpected_node=unexpected,
            heading_change_deg=heading_change,
            nearest_waypoint_m=nearest_wp,
        )
