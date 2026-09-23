"""Shared test helpers: reconstruct AisScoreIn events from the synthetic replay fixture."""

from __future__ import annotations

from datetime import datetime

from app.models import AisFix, AisScoreIn


def as_fix(r) -> AisFix:
    return AisFix(lat=r.lat, lon=r.lon, t=r.t, sog=r.sog, cog=r.cog, heading=r.heading)


def build_score_in(by_mmsi: dict, mmsi: int, t_iso: str) -> AisScoreIn:
    """Reconstruct the AisScoreIn for a vessel's event at t_iso from the fixture."""

    t = datetime.fromisoformat(t_iso.replace("Z", "+00:00"))
    history = [as_fix(r) for r in by_mmsi[mmsi] if r.t < t]
    current = next(as_fix(r) for r in by_mmsi[mmsi] if r.t == t)
    return AisScoreIn(mmsi=mmsi, fix=current, history=history)
