"""orchestrator._pidpm_scorer_adapter (Phase 3, gate 3.6): None when the
PiDpmClient is disabled (so GapDetectorAgent's pi_dpm_scorer stays None and
Phase 1/2 goldens are unaffected); a real closure, adapting a Prism into a
[[lat, lon], [lat, lon]] trajectory, when enabled.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from app.components.space_time_prism import Prism, speed_bounds_for
from app.models import Anchor, AnchorPair
from app.orchestrator import _pidpm_scorer_adapter
from app.pidpm_client import PiDpmClient

T0 = datetime(2024, 6, 1, tzinfo=UTC)


def _prism() -> Prism:
    pair = AnchorPair(
        a=Anchor(lat=40.30, lon=-74.15, t=T0),
        b=Anchor(lat=40.32, lon=-74.14, t=T0 + timedelta(minutes=20)),
    )
    bounds = speed_bounds_for("vessel", vessel_v_max_kts=25.0, vehicle_v_max_kmh=130.0)
    return Prism.compute(pair, bounds)


def test_adapter_is_none_when_client_disabled():
    disabled = PiDpmClient(sagemaker_client=None, s3_client=None, endpoint_name="", input_bucket="")
    assert _pidpm_scorer_adapter(disabled) is None


def test_adapter_converts_a_prism_to_the_frozen_scorer_trajectory_shape():
    calls = []

    class FakeEnabledClient:
        enabled = True

        async def ascore(self, trajectory):
            calls.append(trajectory)
            return 0.55

    scorer = _pidpm_scorer_adapter(FakeEnabledClient())
    assert scorer is not None

    result = asyncio.run(scorer(_prism()))
    assert result == 0.55
    assert calls == [[[40.30, -74.15], [40.32, -74.14]]]
