"""Drill L2: a candidate that passes holdout AND shadow regresses only at a
later canary weight in the local promotion state machine, and the
burn-rate-triggered auto-rollback catches it (Phase 3, gate 3.8; war story
P12 in docs/WAR_STORIES.md).

Neither the offline holdout set nor the shadow window's sampled traffic
happens to cover a rare input distribution the candidate mishandles; both
checks pass cleanly. Only once canary traffic widens past the shadow
sample's coverage does the SLO error budget start burning. Runs the REAL
mlops.promote.run_promotion state machine (not a re-implementation) with an
injected burn_check standing in for serving/app/slo.py's real burn-rate
read, demonstrating the saga's compensating action (DR-3): an immediate,
full revert to the prior champion weight, never a partial one.

No AWS, no SageMaker; the promotion state machine itself is exercised for
real. Transcript to docs/drills/L2_canary_rollback.md; exit 0 only if the
gate/shadow both pass, the canary reaches the regression weight, and the
rollback is complete and immediate.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

import numpy as np  # noqa: E402

from mlops.holdout_gate import run_holdout_gate  # noqa: E402
from mlops.promote import CANARY_WEIGHTS, run_promotion  # noqa: E402
from mlops.shadow_diff import score_diff  # noqa: E402

TRANSCRIPT = REPO_ROOT / "docs" / "drills" / "L2_canary_rollback.md"

REGRESSION_WEIGHT = 25  # the canary step where the drill's synthetic regression first appears


def main() -> int:
    rng = np.random.default_rng(11)
    n = 2000

    # --- a clean holdout gate: the candidate looks good on the offline set ---
    labels = rng.integers(0, 2, size=n)
    scores = labels * 0.9 + rng.normal(0, 0.1, size=n)
    predicted_mean = rng.normal(0, 1, size=n)
    predicted_sigma = np.full(n, 1.0)
    observed = predicted_mean + rng.normal(0, 1, size=n)
    gate = run_holdout_gate(
        labels=labels,
        scores=scores,
        predicted_mean=predicted_mean,
        predicted_sigma=predicted_sigma,
        observed=observed,
    )

    # --- a clean shadow window: the shadow SAMPLE never happens to include
    # the rare regression-triggering input, so champion and shadow agree ---
    champion_scores = rng.normal(0.3, 0.05, size=300)
    shadow_scores = champion_scores + rng.normal(0, 0.01, size=300)
    shadow = score_diff(champion_scores, shadow_scores, max_divergence=0.05)

    # --- canary: an injected burn_check standing in for the real
    # serving/app/slo.py error-budget read; regression only surfaces once
    # traffic widens past what the shadow window happened to sample ---
    weights_probed: list[int] = []

    def burn_check(weight: int) -> bool:
        weights_probed.append(weight)
        return weight == REGRESSION_WEIGHT

    reverted = {"called": False}

    def revert_to_champion() -> None:
        reverted["called"] = True

    weights_set: list[int] = []
    promotion = run_promotion(
        holdout_result=gate,
        shadow_result=shadow,
        burn_check=burn_check,
        set_canary_weight=weights_set.append,
        revert_to_champion=revert_to_champion,
    )

    expected_weights = [w for w in CANARY_WEIGHTS if w <= REGRESSION_WEIGHT]
    clean_before_regression = weights_set == expected_weights
    reached_regression = REGRESSION_WEIGHT in weights_probed
    full_immediate_revert = (
        reverted["called"]
        and promotion.final_status == "rolled_back"
        and promotion.steps[-1].stage == f"canary_{REGRESSION_WEIGHT}"
        and promotion.steps[-1].action == "revert"
        and weights_set[-1] == REGRESSION_WEIGHT  # never advanced past the burning step
    )

    verdict_pass = (
        gate.passed and shadow.passed and clean_before_regression and full_immediate_revert
    )
    verdict = (
        f"PASS (gate + shadow both clean; canary caught the regression at "
        f"weight={REGRESSION_WEIGHT} and reverted immediately and fully, never advancing further)"
        if verdict_pass
        else (
            "FAIL (drill did not reproduce the intended "
            "clean-gate/clean-shadow/canary-catches-it contrast)"
        )
    )

    lines = [
        f"# Drill L2 transcript: canary regression + auto-rollback "
        f"({datetime.now(UTC).isoformat()})",
        "",
        "## Holdout gate (clean)",
        f"AUC={gate.auc:.4f} CRPS={gate.crps:.4f} calibration_ratio={gate.calibration_ratio:.4f}",
        f"PASSED: {gate.passed}",
        "",
        "## Shadow window (clean: the sample never covers the regression input)",
        f"mean_abs_diff={shadow.mean_abs_diff:.4f} PASSED: {shadow.passed}",
        "",
        "## Canary ramp",
        f"weights set in order: {weights_set}",
        f"regression first visible at weight={REGRESSION_WEIGHT}: {reached_regression}",
        f"final_status: {promotion.final_status}",
        f"transition sequence: {[(s.stage, s.action) for s in promotion.steps]}",
        f"revert_to_champion called: {reverted['called']}",
        "",
        f"VERDICT: {verdict}",
    ]
    TRANSCRIPT.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))

    ok = gate.passed and shadow.passed and clean_before_regression and full_immediate_revert
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
