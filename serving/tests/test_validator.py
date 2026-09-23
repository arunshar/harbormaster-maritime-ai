"""ValidatorAgent S-KBM gate tests (Phase 1.2). Impossible inputs -> 422."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.agents.validator import ValidatorAgent
from app.config import Settings
from app.errors import KinematicViolation
from app.models import Anchor, RendezvousRegion

T0 = datetime(2024, 1, 1, tzinfo=UTC)


async def test_impossible_anchor_speed_raises_422():
    v = ValidatorAgent(Settings())
    anchors = [
        Anchor(lat=40.0, lon=-74.0, t=T0),
        Anchor(lat=41.5, lon=-72.0, t=T0 + timedelta(minutes=1)),  # ~200 km in 60 s
    ]
    with pytest.raises(KinematicViolation) as exc:
        await v.validate([], domain="vessel", anchors=anchors)
    assert exc.value.http_status == 422


async def test_reversed_region_window_raises_422():
    v = ValidatorAgent(Settings())
    bad = RendezvousRegion(
        polygon_geojson={"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [0, 0]]]},
        earliest_meet_t=T0 + timedelta(hours=2),
        latest_meet_t=T0,  # latest before earliest
        confidence=0.5,
        method="TGARD",
    )
    with pytest.raises(KinematicViolation):
        await v.validate([bad], domain="vessel")


async def test_feasible_anchors_pass():
    v = ValidatorAgent(Settings())
    anchors = [
        Anchor(lat=40.50, lon=-73.95, t=T0),
        Anchor(lat=40.58, lon=-73.95, t=T0 + timedelta(hours=1)),  # reachable at 25 kts
    ]
    assert await v.validate([], domain="vessel", anchors=anchors) == []
