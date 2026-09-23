"""GapDetectorAgent's injected pi_dpm_scorer (Phase 3, gate 3.6): a real
score overrides the analytic P_data estimate; a scorer returning None (or no
scorer at all, the Phase 1/2 default) falls back to the existing analytic
estimate untouched. No SageMaker, no AWS: the scorer is a plain injected
async callable.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.agents.gap_detector import GapDetectorAgent, _mobr_bounds_overlap
from app.components.space_time_prism import Prism
from app.config import Settings
from app.models import Anchor, AnchorPair, SpeedBounds

T0 = datetime(2024, 6, 1, tzinfo=UTC)


def _gappy_trajectory() -> list[Anchor]:
    # a 20-minute silence (> the 600s coverage_threshold_s default) over a
    # physically reachable distance at 25 kts
    return [
        Anchor(lat=40.30, lon=-74.15, t=T0),
        Anchor(lat=40.32, lon=-74.14, t=T0 + timedelta(minutes=20)),
    ]


@pytest.mark.asyncio
async def test_no_scorer_uses_the_analytic_estimate_phase1_default():
    agent = GapDetectorAgent(Settings())
    gaps = await agent.detect({"trajectory": _gappy_trajectory()})
    assert len(gaps) == 1
    analytic = gaps[0].p_data
    assert 0.0 <= analytic <= 1.0


@pytest.mark.asyncio
async def test_a_real_scorer_result_overrides_the_analytic_estimate():
    async def fake_scorer(prism):
        return 0.99

    agent = GapDetectorAgent(Settings(), pi_dpm_scorer=fake_scorer)
    gaps = await agent.detect({"trajectory": _gappy_trajectory()})
    assert len(gaps) == 1
    assert gaps[0].p_data == 0.99


@pytest.mark.asyncio
async def test_a_scorer_returning_none_falls_back_to_the_analytic_estimate():
    calls = []

    async def failing_scorer(prism):
        calls.append(prism)
        return None  # disabled/timed-out/failed: PiDpmClient's contract

    agent_with_none = GapDetectorAgent(Settings(), pi_dpm_scorer=failing_scorer)
    agent_without_scorer = GapDetectorAgent(Settings())

    gaps_a = await agent_with_none.detect({"trajectory": _gappy_trajectory()})
    gaps_b = await agent_without_scorer.detect({"trajectory": _gappy_trajectory()})

    assert len(calls) == 1  # the scorer was actually invoked
    assert gaps_a[0].p_data == gaps_b[0].p_data  # identical to the no-scorer path
    assert gaps_a[0].abnormal_gap_measure == gaps_b[0].abnormal_gap_measure


@pytest.mark.asyncio
async def test_transitive_gap_prisms_merge_into_one_connected_region():
    """Wave 3 finding: the DRM merge must be connected-components, not single-pass
    seed clustering. Three consecutive gaps whose prism MOBRs chain (0 overlaps 1,
    1 overlaps 2, 0 disjoint from 2) are ONE region, not two: seed clustering
    double-counted the middle prism and dropped the tail one."""
    # a vessel stepping north, 2000s (> 600s threshold) between each fix, so each
    # consecutive pair is a gap and the gap prisms overlap transitively
    traj = [
        Anchor(lat=0.0 + 0.2 * i, lon=0.0, t=T0 + timedelta(seconds=2000 * i)) for i in range(4)
    ]
    gaps = await GapDetectorAgent(Settings()).detect({"trajectory": traj, "domain": "vessel"})
    assert len(gaps) == 1, f"expected one connected region, got {len(gaps)} (seed-cluster bug)"
    # the head is the earliest gap; the later gaps are folded into its coverage,
    # not emitted as separate (previously double-counted / dropped) records
    assert gaps[0].start.lat == 0.0
    assert gaps[0].end.lat == pytest.approx(0.2)


def test_antimeridian_mobr_components_do_not_overlap_greenwich_bounds():
    bounds = SpeedBounds(v_max_mps=20.0)
    dateline = Prism.compute(
        AnchorPair(
            a=Anchor(lat=0.0, lon=179.9, t=T0),
            b=Anchor(lat=0.0, lon=-179.9, t=T0 + timedelta(hours=2)),
        ),
        bounds,
    ).mobr()
    greenwich = Prism.compute(
        AnchorPair(
            a=Anchor(lat=0.0, lon=-0.1, t=T0),
            b=Anchor(lat=0.0, lon=0.1, t=T0 + timedelta(hours=2)),
        ),
        bounds,
    ).mobr()
    seam_neighbor = Prism.compute(
        AnchorPair(
            a=Anchor(lat=0.0, lon=-179.95, t=T0),
            b=Anchor(lat=0.0, lon=-179.75, t=T0 + timedelta(hours=2)),
        ),
        bounds,
    ).mobr()
    exact_seam = Prism.compute(
        AnchorPair(
            a=Anchor(lat=0.0, lon=180.0, t=T0),
            b=Anchor(lat=0.0, lon=-180.0, t=T0 + timedelta(hours=2)),
        ),
        bounds,
    ).mobr()

    assert not _mobr_bounds_overlap(dateline, greenwich)
    assert _mobr_bounds_overlap(dateline, seam_neighbor)
    assert _mobr_bounds_overlap(exact_seam, seam_neighbor)


@pytest.mark.asyncio
async def test_antimeridian_gap_uses_short_distance_and_cut_geojson():
    traj = [
        Anchor(lat=0.0, lon=179.9, t=T0),
        Anchor(lat=0.0, lon=-179.9, t=T0 + timedelta(minutes=40)),
    ]

    gaps = await GapDetectorAgent(Settings()).detect({"trajectory": traj, "domain": "vessel"})

    assert len(gaps) == 1
    assert gaps[0].distance_m == pytest.approx(22_238.985_329, rel=1e-9)
    assert gaps[0].p_physical == 1.0
    assert gaps[0].coverage_polygon_geojson["type"] == "MultiPolygon"


@pytest.mark.asyncio
async def test_infeasible_gap_retains_a_finite_degenerate_coverage_polygon():
    traj = [
        Anchor(lat=0.0, lon=0.0, t=T0),
        Anchor(lat=0.0, lon=0.1, t=T0 + timedelta(seconds=601)),
    ]

    gaps = await GapDetectorAgent(Settings()).detect({"trajectory": traj, "domain": "vessel"})

    assert len(gaps) == 1
    assert gaps[0].p_physical < 1.0
    assert gaps[0].coverage_polygon_geojson["type"] == "Polygon"
