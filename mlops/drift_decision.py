"""Phase 4 gate 4.3: the drift decision table, made checkable.

Promotes docs/phases/PHASE_4_SKETCH.md's decision table (defensibility rule,
verbatim there) into one pure function. Precedence, highest first: concept
drift (proxy 2, the HITL disagreement rate) overrides everything else, since
it is the only proxy with a real, if lagging, ground-truth-adjacent signal;
calibration drift is next; input drift alone is log-only.

The concept-drift disagreement-rate threshold below is a NAMED PLACEHOLDER,
not an empirical result: docs/phases/PHASE_4.md's sprint-honest scope states
this explicitly. It exists so the mechanism is testable; replace it with a
value derived from a real accumulated HITL-verdict baseline before this gate
drives any real retraining decision.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from mlops.calibration_watch import CalibrationWatchResult
from mlops.concept_proxy import DisagreementResult
from mlops.drift import DriftResult

# TODO(real-phase-4-execution): the derivation path for this placeholder is
# now built: mlops/disagreement_baseline.py accumulates per-window HITL
# disagreement rates and make_disagreement_alert_rate() derives the alert
# threshold, which callers pass in through classify_drift's
# disagreement_alert_rate parameter below. What is still pending is the real
# accumulated HITL data itself, per PHASE_4.md's sprint-honest scope; until
# enough real windows exist, make_disagreement_alert_rate falls back to this
# value, which only makes the mechanism testable, it is not a result.
DISAGREEMENT_ALERT_RATE_PLACEHOLDER = 0.2

DriftCategory = Literal["none", "input_drift", "calibration_drift", "concept_drift"]
DriftResponse = Literal["log_only", "supervised_retrain", "preference_pipeline"]


@dataclass(frozen=True)
class DriftDecision:
    category: DriftCategory
    response: DriftResponse
    reason: str


def classify_drift(
    *,
    input_results: list[DriftResult],
    calibration: CalibrationWatchResult,
    disagreement: DisagreementResult,
    disagreement_alert_rate: float = DISAGREEMENT_ALERT_RATE_PLACEHOLDER,
) -> DriftDecision:
    """The cross-validation rule (verbatim from the sketch): a rising
    disagreement rate (proxy 2) is concept drift regardless of proxy 1's
    near-threshold+high-variance volume; a proxy-1 signal alone, with
    disagreement flat, is NOT concept drift (more ambiguous traffic the
    model is still right about, log only)."""
    if disagreement.n > 0 and disagreement.rate >= disagreement_alert_rate:
        return DriftDecision(
            category="concept_drift",
            response="preference_pipeline",
            reason=(
                f"HITL disagreement rate {disagreement.rate:.3f} "
                f">= alert threshold {disagreement_alert_rate} over {disagreement.n} labeled rows"
            ),
        )

    if calibration.response == "supervised_retrain":
        return DriftDecision(
            category="calibration_drift",
            response="supervised_retrain",
            reason=calibration.reason,
        )

    if any(r.drifted for r in input_results):
        drifted_features = ", ".join(r.feature for r in input_results if r.drifted)
        return DriftDecision(
            category="input_drift",
            response="log_only",
            reason=f"input drift on: {drifted_features}; calibration and disagreement flat",
        )

    return DriftDecision(category="none", response="log_only", reason="no drift signal")
