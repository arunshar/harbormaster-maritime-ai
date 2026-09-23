"""The promotion state machine (Phase 3, gate 3.7): holdout gate -> shadow ->
weighted canary 5/25/50/100, with burn-rate-triggered auto-rollback.

Encodes DR-3's target promotion decision sequence
(docs/SYSTEM_DESIGN_DECISIONS.md). The current Phase 3 asynchronous endpoint
has one production variant, so this module does not call SageMaker or enact a
live traffic shift. Every I/O boundary (setting a canary weight, checking the
SLO burn-rate, reverting to the prior champion) is an injected callable, so
the policy state machine remains fully unit-testable with zero AWS. A future
real candidate comparison needs a separately reviewed endpoint design and
control-plane implementation.

Invariant 3 (docs/phases/PHASE_3.md): canary weight only ever increases
while the burn-rate stays healthy; a breach at any step reverts the FULL
weight to the prior champion in one action, never a partial rollback.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from app.burn_rate import Bucket, evaluate_burn_rate
from mlops.holdout_gate import HoldoutGateResult
from mlops.reward_hacking_probe import RewardHackingProbeResult
from mlops.shadow_diff import ShadowDiffResult

CANARY_WEIGHTS: tuple[int, ...] = (5, 25, 50, 100)


@dataclass(frozen=True)
class PromotionStep:
    stage: str
    action: str  # "pass" | "fail" | "advance" | "revert"


@dataclass(frozen=True)
class PromotionResult:
    steps: list[PromotionStep] = field(default_factory=list)
    final_status: str = "rejected_gate"  # | ... | "rejected_reward_probe" | ... | "promoted"


def run_promotion(
    *,
    holdout_result: HoldoutGateResult,
    shadow_result: ShadowDiffResult | None,
    burn_check: Callable[[int], bool],
    set_canary_weight: Callable[[int], None],
    revert_to_champion: Callable[[], None],
    reward_hacking_result: RewardHackingProbeResult | None = None,
) -> PromotionResult:
    """burn_check(weight) -> True means the SLO error budget is burning at
    that canary weight; False means healthy. The live run builds this from the
    real multi-window burn-rate calculator via make_burn_check below
    (serving/app/slo.py's machinery, DR-13); tests inject a canned callable. A
    candidate that fails the holdout gate never reaches shadow or canary at all
    (invariant 1); the same holds for a failed/missing shadow result (a
    candidate cannot skip shadow to reach canary).

    Phase 4 gate 4.5, additive: reward_hacking_result is None for every
    supervised-retrain candidate (there is no reward function in that path
    to game), which reproduces the exact pre-Phase-4 step sequence and
    final_status values unchanged - the pinned clean-run transitions in
    mlops/fixtures/expectations.json still hold byte-for-byte. Only a
    preference-tuned candidate supplies a real result, checked immediately
    after the holdout gate and before shadow: a blocked probe halts
    promotion exactly the way a failed gate does, one new step
    ("reward_probe", "fail"), no partial states."""
    steps: list[PromotionStep] = [
        PromotionStep(stage="gate", action="pass" if holdout_result.passed else "fail")
    ]
    if not holdout_result.passed:
        return PromotionResult(steps=steps, final_status="rejected_gate")

    if reward_hacking_result is not None:
        steps.append(
            PromotionStep(
                stage="reward_probe", action="fail" if reward_hacking_result.blocked else "pass"
            )
        )
        if reward_hacking_result.blocked:
            return PromotionResult(steps=steps, final_status="rejected_reward_probe")

    shadow_passed = shadow_result is not None and shadow_result.passed
    steps.append(PromotionStep(stage="shadow", action="pass" if shadow_passed else "fail"))
    if not shadow_passed:
        return PromotionResult(steps=steps, final_status="rejected_shadow")

    for weight in CANARY_WEIGHTS:
        set_canary_weight(weight)
        if burn_check(weight):
            revert_to_champion()
            steps.append(PromotionStep(stage=f"canary_{weight}", action="revert"))
            return PromotionResult(steps=steps, final_status="rolled_back")
        steps.append(PromotionStep(stage=f"canary_{weight}", action="advance"))

    return PromotionResult(steps=steps, final_status="promoted")


# --- The real burn_check: fed by the DR-13 calculator, not a hardcoded False --
#
# run_promotion still takes an injected burn_check(weight) -> bool so the state
# machine stays fully unit-testable with zero AWS (and the pinned Phase 3/4
# transition sequences hold byte-for-byte). What changes is where the LIVE
# promotion run gets that callable: instead of `lambda w: False`, it builds one
# from serving/app/slo.py's multi-window burn-rate calculator over a per-weight
# metrics series. `series_provider(weight)` yields the aggregated
# (timestamp, total, bad) score-success buckets observed at that canary weight;
# in a test it is a plain function returning a canned series, in the AWS demo
# window it is a thin CloudWatch GetMetricData adapter (the burn_rate module's
# SeriesProvider seam). A PAGE-severity result (fast- or slow-burn) is exactly
# the auto-rollback trigger DR-3 calls the promotion saga's compensating action.


def make_burn_check(
    series_provider: Callable[[int], Sequence[Bucket]],
    *,
    bucket_seconds: float,
    target: float = 0.999,
) -> Callable[[int], bool]:
    """Build a real ``burn_check(weight) -> bool`` for ``run_promotion``.

    Returns True (budget is burning, revert) when the multi-window calculator
    raises a PAGE for the score-success SLO at that weight; a WARNING or OK
    returns False, so a ticket-grade slow bleed does not trigger a rollback.
    """

    def burn_check(weight: int) -> bool:
        buckets = series_provider(weight)
        result = evaluate_burn_rate(buckets, target=target, bucket_seconds=bucket_seconds)
        return result.should_rollback

    return burn_check
