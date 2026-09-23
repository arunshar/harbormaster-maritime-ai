"""Phase 4 gate 4.3: the two concept-drift proxies.

There is no ground-truth label stream in production anomaly detection, so
concept drift is detected only by proxy, never directly (docs/phases/
PHASE_4_SKETCH.md is explicit about this limit). Two proxies, meant to be
cross-validated against each other, never trusted alone (mlops/
drift_decision.py does the cross-validation; this module only computes the
two raw signals).
"""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_BAND_HALFWIDTH = 0.05
DEFAULT_VARIANCE_FLOOR = 0.1


def flag_uncertain_trace(
    score: float,
    epistemic_variance: float | None,
    *,
    hitl_threshold: float,
    band_halfwidth: float = DEFAULT_BAND_HALFWIDTH,
    variance_floor: float = DEFAULT_VARIANCE_FLOOR,
) -> bool:
    """Proxy 1: near-threshold AND high-epistemic-uncertainty. A trace with
    no variance signal (epistemic_variance is None: the demo stand-in or any
    checkpoint without an uncertainty head) can never be flagged by this
    proxy - "no signal" is not "high uncertainty"."""
    if epistemic_variance is None:
        return False
    near_threshold = abs(score - hitl_threshold) <= band_halfwidth
    high_variance = epistemic_variance >= variance_floor
    return near_threshold and high_variance


@dataclass(frozen=True)
class DisagreementResult:
    rate: float
    n: int
    excluded: int


def disagreement_rate(
    rows,
    *,
    hitl_threshold: float,
) -> DisagreementResult:
    """Proxy 2: the rate at which operators override the model's implied
    verdict, over hitl_queue-shaped rows (each a mapping with at least
    "score" and "label"). The model-implied verdict is score >= hitl_threshold
    (every row that legitimately reaches the HITL queue was flagged
    anomalous, so this should always hold; a row where it does not is
    excluded as a data inconsistency rather than silently miscounted).
    Rows with label in {None, "ambiguous"} are excluded, exactly as
    the HITL-loading logic in an earlier reinforcement-learning project of
    mine excludes ambiguous items. "incorrect"
    means the operator disagreed with the model's anomalous verdict."""
    rows = list(rows)
    valid = [
        r
        for r in rows
        if r.get("label") in ("correct", "incorrect") and r.get("score", 0.0) >= hitl_threshold
    ]
    excluded = len(rows) - len(valid)
    n = len(valid)
    if n == 0:
        return DisagreementResult(rate=0.0, n=0, excluded=excluded)
    disagreements = sum(1 for r in valid if r["label"] == "incorrect")
    return DisagreementResult(rate=disagreements / n, n=n, excluded=excluded)
