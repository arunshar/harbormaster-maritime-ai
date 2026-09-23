"""ValidatorAgent. Hard kinematic invariant gate.

Every region returned to the user must pass through this agent. Failure
raises `KinematicViolation`, which the orchestrator surfaces as HTTP 422.
"""

from __future__ import annotations

from itertools import pairwise

from app.components.space_time_prism import haversine_m, speed_bounds_for
from app.config import Settings
from app.errors import KinematicViolation
from app.models import Anchor, RendezvousRegion


class ValidatorAgent:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def validate(
        self,
        regions: list[RendezvousRegion],
        *,
        domain: str = "vessel",
        anchors: list[Anchor] | None = None,
    ) -> list[RendezvousRegion]:
        bounds = speed_bounds_for(
            domain,
            vessel_v_max_kts=self.settings.vessel_v_max_kts,
            vehicle_v_max_kmh=self.settings.vehicle_v_max_kmh,
        )
        # Hard kinematic invariant on the OBSERVATIONS (the Hagerstrand prism
        # feasibility condition): consecutive anchors must be mutually reachable
        # under v_max, i.e. dist(A, B) <= v_max * (t_B - t_A). This is the real
        # speed gate. A rendezvous region's polygon is the set of alternative
        # meeting points, NOT a path to be traversed, so its bounding-box
        # diagonal is not a "required speed" and must not be gated on: a region's
        # spatial extent scales with the prism duration (v_max * duration) while
        # the meet window is shorter, so that proxy rejected every feasible region.
        if anchors:
            ordered = sorted(anchors, key=lambda a: a.t)
            for a, b in pairwise(ordered):
                dt_s = (b.t - a.t).total_seconds()
                if dt_s <= 0:
                    continue
                v_req = haversine_m(a.lat, a.lon, b.lat, b.lon) / dt_s
                if v_req > bounds.v_max_mps * 1.05:
                    raise KinematicViolation(
                        "anchors imply infeasible required speed",
                        v_req=v_req, v_max=bounds.v_max_mps,
                    )
        out: list[RendezvousRegion] = []
        for r in regions:
            window_s = (r.latest_meet_t - r.earliest_meet_t).total_seconds()
            if window_s < 0:
                raise KinematicViolation(
                    "region time window is reversed", region=r.model_dump(mode="json")
                )
            out.append(r)
        return out
