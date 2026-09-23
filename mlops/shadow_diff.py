"""Paired-score comparator (Phase 3, gate 3.7).

Compares two already-collected score arrays. It performs no SageMaker call,
request mirroring, traffic shift, or endpoint creation. SageMaker managed
shadow tests are unavailable for endpoints using Asynchronous Inference, and
the current Terraform configuration rejects a candidate artifact on its
one-variant async endpoint. Consequently, this function is local policy and
test evidence only, not a managed shadow implementation or proof of live model
quality. A future real comparison requires distinct artifacts, a separately
reviewed endpoint design, and external authorization.

"Clean" means the mean absolute divergence stays under a threshold over the
paired-score window. This check can catch training-serving skew (drill 3.8-L1),
which the holdout gate alone cannot see because it evaluates the candidate
against the same offline-encoded holdout set the skew itself would have
produced.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class ShadowDiffResult:
    mean_abs_diff: float
    max_abs_diff: float
    n_samples: int
    passed: bool


def score_diff(
    champion_scores: np.ndarray, shadow_scores: np.ndarray, *, max_divergence: float
) -> ShadowDiffResult:
    champion_scores = np.asarray(champion_scores, dtype=float)
    shadow_scores = np.asarray(shadow_scores, dtype=float)
    if len(champion_scores) != len(shadow_scores):
        raise ValueError("champion and shadow score arrays must be paired 1:1, same length")
    if len(champion_scores) == 0:
        raise ValueError("score_diff needs at least one paired sample")

    diffs = np.abs(champion_scores - shadow_scores)
    mean_diff = float(np.mean(diffs))
    return ShadowDiffResult(
        mean_abs_diff=mean_diff,
        max_abs_diff=float(np.max(diffs)),
        n_samples=len(diffs),
        passed=mean_diff <= max_divergence,
    )
