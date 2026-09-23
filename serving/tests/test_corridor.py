"""CorridorDeviationDetector tests (Phase 1.2): perpendicular lane association."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.agents.corridor_detector import CorridorDeviationDetector
from app.config import Settings
from app.models import Anchor

T0 = datetime(2024, 1, 1, tzinfo=UTC)


async def test_on_corridor_point_passes():
    det = CorridorDeviationDetector(Settings())
    on_lane = Anchor(lat=40.40, lon=-74.10, t=T0)  # a lane node from corridors.json
    finding = await det.detect([on_lane])
    assert finding.off_corridor is False
    assert finding.distance_m < Settings().off_corridor_threshold_m


async def test_off_corridor_point_flagged():
    det = CorridorDeviationDetector(Settings())
    off = Anchor(lat=39.5, lon=-72.0, t=T0)  # well away from the lane
    finding = await det.detect([off])
    assert finding.off_corridor is True
    assert finding.distance_m > Settings().off_corridor_threshold_m


async def test_unexpected_node_on_sharp_turn_away_from_waypoints():
    det = CorridorDeviationDetector(Settings())
    # three fixes with a ~90 deg course change, far from any waypoint node
    track = [
        Anchor(lat=39.50, lon=-72.00, t=T0),
        Anchor(lat=39.51, lon=-72.00, t=T0 + timedelta(minutes=1)),  # heading ~north
        Anchor(lat=39.51, lon=-71.98, t=T0 + timedelta(minutes=2)),  # turns ~east
    ]
    finding = await det.detect(track)
    assert finding.unexpected_node is True
    assert finding.heading_change_deg > Settings().unexpected_node_heading_deg
