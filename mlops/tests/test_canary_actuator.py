from __future__ import annotations

from typing import Any

import pytest

from mlops.canary_actuator import make_revert_to_champion, make_set_canary_weight
from mlops.holdout_gate import HoldoutGateResult
from mlops.promote import CANARY_WEIGHTS, run_promotion
from mlops.shadow_diff import ShadowDiffResult

ENDPOINT = "harbormaster-base-pidpm"

PASSING_GATE = HoldoutGateResult(
    auc=0.95, crps=0.2, calibration_ratio=1.0, passed=True, failures=[]
)
CLEAN_SHADOW = ShadowDiffResult(mean_abs_diff=0.01, max_abs_diff=0.02, n_samples=100, passed=True)


class _FakeWeightsClient:
    """Recording fake for the injected SageMaker client (never real boto3)."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def update_endpoint_weights_and_capacities(self, **kwargs: Any) -> dict:
        self.calls.append(kwargs)
        return {"EndpointArn": f"arn:aws:sagemaker:us-east-1:000000000000:endpoint/{ENDPOINT}"}


class _RaisingClient:
    """Fail-closed check: the actuator must propagate, never swallow."""

    def update_endpoint_weights_and_capacities(self, **kwargs: Any) -> dict:
        raise RuntimeError("ThrottlingException: Rate exceeded")


@pytest.mark.parametrize("weight", CANARY_WEIGHTS)
def test_each_ladder_weight_maps_to_the_exact_candidate_champion_pair(weight):
    fake = _FakeWeightsClient()
    set_canary_weight = make_set_canary_weight(fake, ENDPOINT)
    set_canary_weight(weight)
    assert fake.calls == [
        {
            "EndpointName": ENDPOINT,
            "DesiredWeightsAndCapacities": [
                {"VariantName": "candidate", "DesiredWeight": weight / 100},
                {"VariantName": "champion", "DesiredWeight": 1 - weight / 100},
            ],
        }
    ]


def test_weight_100_fully_drains_the_champion():
    fake = _FakeWeightsClient()
    make_set_canary_weight(fake, ENDPOINT)(100)
    (call,) = fake.calls
    assert call["DesiredWeightsAndCapacities"] == [
        {"VariantName": "candidate", "DesiredWeight": 1.0},
        {"VariantName": "champion", "DesiredWeight": 0.0},
    ]


def test_variant_names_are_injectable():
    fake = _FakeWeightsClient()
    set_canary_weight = make_set_canary_weight(fake, ENDPOINT, champion="blue", candidate="green")
    set_canary_weight(50)
    (call,) = fake.calls
    names = [pair["VariantName"] for pair in call["DesiredWeightsAndCapacities"]]
    assert names == ["green", "blue"]


def test_revert_is_one_call_restoring_champion_full_weight():
    fake = _FakeWeightsClient()
    revert = make_revert_to_champion(fake, ENDPOINT)
    revert()
    # invariant 3: exactly ONE call, full weight back to the champion
    assert fake.calls == [
        {
            "EndpointName": ENDPOINT,
            "DesiredWeightsAndCapacities": [
                {"VariantName": "champion", "DesiredWeight": 1.0},
                {"VariantName": "candidate", "DesiredWeight": 0.0},
            ],
        }
    ]


def test_a_raising_client_propagates_from_set_canary_weight():
    set_canary_weight = make_set_canary_weight(_RaisingClient(), ENDPOINT)
    with pytest.raises(RuntimeError, match="Rate exceeded"):
        set_canary_weight(5)


def test_a_raising_client_propagates_from_revert_to_champion():
    revert = make_revert_to_champion(_RaisingClient(), ENDPOINT)
    with pytest.raises(RuntimeError, match="Rate exceeded"):
        revert()


def test_run_promotion_wired_to_the_real_actuators_rolls_back_on_a_burn_at_25():
    # The full seam: run_promotion's injected callables ARE the actuators,
    # a burn at weight 25 rolls back, and the fake saw 5, then 25, then the
    # single full-weight revert, nothing after.
    fake = _FakeWeightsClient()
    result = run_promotion(
        holdout_result=PASSING_GATE,
        shadow_result=CLEAN_SHADOW,
        burn_check=lambda w: w == 25,
        set_canary_weight=make_set_canary_weight(fake, ENDPOINT),
        revert_to_champion=make_revert_to_champion(fake, ENDPOINT),
    )
    assert result.final_status == "rolled_back"
    assert result.steps[-1].stage == "canary_25"
    assert result.steps[-1].action == "revert"
    assert [call["DesiredWeightsAndCapacities"] for call in fake.calls] == [
        [
            {"VariantName": "candidate", "DesiredWeight": 0.05},
            {"VariantName": "champion", "DesiredWeight": 0.95},
        ],
        [
            {"VariantName": "candidate", "DesiredWeight": 0.25},
            {"VariantName": "champion", "DesiredWeight": 0.75},
        ],
        [
            {"VariantName": "champion", "DesiredWeight": 1.0},
            {"VariantName": "candidate", "DesiredWeight": 0.0},
        ],
    ]
