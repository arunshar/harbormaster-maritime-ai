"""Drill L1: training-serving skew, caught by shadow, not by holdout (Phase
3, gate 3.8; war story P11 in docs/WAR_STORIES.md).

A candidate's offline training-set export and its online serving path
encode a feature differently: the offline export always standardizes
(feature - mean) / std before scoring (matching how the toy model here was
"calibrated"), but a skew bug in the online path passes the RAW feature
value instead. The holdout gate runs entirely against the offline-encoded
holdout set, so it cannot see the mismatch: it PASSES. Shadow, run against
the SAME live events through the real online encoding path, catches it
immediately: the shadow variant's (skewed) scores diverge wildly from the
champion's (correctly encoded) scores on identical inputs.

Uses the real mlops.holdout_gate.run_holdout_gate and mlops.shadow_diff.
score_diff against a synthetic toy scorer and synthetic AIS-shaped gap
durations; no AWS, no SageMaker. Transcript to
docs/drills/L1_training_serving_skew.md; exit 0 only if holdout passes AND
shadow fails, demonstrating the exact contrast the drill exists to show.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

import numpy as np  # noqa: E402

from mlops.holdout_gate import run_holdout_gate  # noqa: E402
from mlops.shadow_diff import score_diff  # noqa: E402

TRANSCRIPT = REPO_ROOT / "docs" / "drills" / "L1_training_serving_skew.md"

FEATURE_MEAN = 900.0  # seconds; the reference population's mean gap duration
FEATURE_STD = 400.0


def toy_scorer(standardized_feature: np.ndarray) -> np.ndarray:
    """The "model": a sigmoid over the STANDARDIZED feature, exactly as it
    was calibrated. Any caller that forgets to standardize gets nonsense."""
    return 1.0 / (1.0 + np.exp(-(standardized_feature - 0.5)))


def main() -> int:
    rng = np.random.default_rng(7)
    n = 2000

    # --- offline holdout set: correctly standardized, as the model expects ---
    raw_gap_s = rng.normal(FEATURE_MEAN, FEATURE_STD, size=n).clip(min=1.0)
    standardized = (raw_gap_s - FEATURE_MEAN) / FEATURE_STD
    holdout_scores = toy_scorer(standardized)
    labels = (holdout_scores > 0.6).astype(int)  # a self-consistent, cleanly separable label
    predicted_mean = holdout_scores
    predicted_sigma = np.full(n, 0.1)
    observed = holdout_scores + rng.normal(0, 0.1, size=n)

    gate = run_holdout_gate(
        labels=labels,
        scores=holdout_scores,
        predicted_mean=predicted_mean,
        predicted_sigma=predicted_sigma,
        observed=observed,
    )

    # --- online: champion (correct encoding) vs shadow (skewed: raw, not standardized) ---
    live_gap_s = rng.normal(FEATURE_MEAN, FEATURE_STD, size=500).clip(min=1.0)
    champion_scores = toy_scorer((live_gap_s - FEATURE_MEAN) / FEATURE_STD)  # correct
    shadow_scores = toy_scorer(live_gap_s)  # BUG: raw seconds fed straight into the sigmoid

    shadow = score_diff(champion_scores, shadow_scores, max_divergence=0.05)

    lines = [
        f"# Drill L1 transcript: training-serving skew ({datetime.now(UTC).isoformat()})",
        "",
        "## Offline holdout gate (evaluated on the correctly-standardized offline export)",
        f"AUC={gate.auc:.4f} CRPS={gate.crps:.4f} calibration_ratio={gate.calibration_ratio:.4f}",
        f"PASSED: {gate.passed} (failures: {gate.failures or 'none'})",
        "",
        "## Online shadow (same live events, real online encoding path)",
        "champion (correct standardization) vs shadow (skew bug: raw seconds, unstandardized)",
        f"mean_abs_diff={shadow.mean_abs_diff:.4f} max_abs_diff={shadow.max_abs_diff:.4f} "
        f"(threshold {0.05})",
        f"PASSED: {shadow.passed}",
        "",
        "VERDICT: "
        + (
            "PASS (holdout cannot see the skew and passes; shadow catches it immediately)"
            if gate.passed and not shadow.passed
            else "FAIL (drill did not reproduce the intended holdout-passes/shadow-fails contrast)"
        ),
    ]
    TRANSCRIPT.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))

    return 0 if (gate.passed and not shadow.passed) else 1


if __name__ == "__main__":
    raise SystemExit(main())
