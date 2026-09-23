"""Vendored RendezvousFinder smoke (Phase 1.2 reuse anchor).

Not routed by the single-vessel HeuristicPlanner, but kept working for the
multi-vessel endpoint: two converging prisms must yield a rendezvous region.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.agents.rendezvous_finder import RendezvousFinderAgent
from app.agents.space_time_reasoner import SpaceTimeReasoner
from app.components.space_time_prism import Prism, speed_bounds_for
from app.config import Settings
from app.models import Anchor, AnchorPair

T0 = datetime(2024, 1, 1, tzinfo=UTC)


async def test_two_converging_prisms_yield_a_region():
    s = Settings()
    bounds = speed_bounds_for(
        "vessel", vessel_v_max_kts=s.vessel_v_max_kts, vehicle_v_max_kmh=s.vehicle_v_max_kmh
    )
    t2 = T0 + timedelta(hours=2)
    # two vessels whose prisms overlap in space and time around (40.55, -73.95)
    p1 = Prism.compute(
        AnchorPair(a=Anchor(lat=40.50, lon=-73.95, t=T0), b=Anchor(lat=40.60, lon=-73.95, t=t2)),
        bounds,
    )
    p2 = Prism.compute(
        AnchorPair(a=Anchor(lat=40.55, lon=-74.02, t=T0), b=Anchor(lat=40.55, lon=-73.88, t=t2)),
        bounds,
    )
    rdv = RendezvousFinderAgent(s, SpaceTimeReasoner(s))
    regions = await rdv.find([p1, p2], method="TGARD")
    assert len(regions) >= 1
    assert regions[0].method == "TGARD"
    assert 0.0 <= regions[0].confidence <= 1.0
