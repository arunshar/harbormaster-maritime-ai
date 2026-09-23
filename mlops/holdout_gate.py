"""The MIRROR-holdout quality gate (Phase 3, gate 3.7).

Ports (does not cross-repo-import) three real metrics from the separate MIRROR project,
matching this repo's established pattern for external reuse (the Phase 1
GeoTrace agents were vendored the same way):

- AUC: the ROC-AUC scoring MIRROR's `eval/generalization.py:run_abc_study`
  performs over genuine-vs-spoofed pairs. Implemented here via the standard
  Mann-Whitney U statistic (rank-based), zero external dependencies.
- CRPS: the closed-form Gneiting & Raftery formula for a Gaussian predictive
  distribution (verified against numerical integration of the CRPS
  definition, not merely assumed correct), an analog of MIRROR's
  ensemble-based `crps_ensemble` appropriate when the candidate's predictive
  uncertainty is a single (mean, sigma) pair rather than a raw ensemble.
- calibration_ratio: the EXACT formula in `serving/monitoring/drift.py:138`,
  `mean(err**2) / mean(variance)`, 1.0 = perfectly calibrated. MIRROR's own
  `check_calibration_drift` flags drift on a symmetric tolerance band
  (default 0.25 around 1.0), not literally the design's `[0.8, 1.2]`
  range; the two happen to coincide at tol=0.2, but this module adds the
  explicit range check as new logic on top of the reused ratio computation,
  not by assuming the tolerance band means the same thing.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

import numpy as np


def roc_auc(labels: np.ndarray, scores: np.ndarray) -> float:
    """Standard ROC-AUC via the Mann-Whitney U statistic: the probability a
    randomly chosen positive scores higher than a randomly chosen negative
    (ties count as half a win). No sklearn dependency."""
    labels = np.asarray(labels)
    scores = np.asarray(scores)
    positives = scores[labels == 1]
    negatives = scores[labels == 0]
    if len(positives) == 0 or len(negatives) == 0:
        raise ValueError("roc_auc needs at least one positive and one negative label")

    ranks = _rankdata(np.concatenate([positives, negatives]))
    rank_sum_pos = ranks[: len(positives)].sum()
    n_pos, n_neg = len(positives), len(negatives)
    u_statistic = rank_sum_pos - n_pos * (n_pos + 1) / 2
    return float(u_statistic / (n_pos * n_neg))


def _rankdata(values: np.ndarray) -> np.ndarray:
    """Average ranks (1-indexed), ties split evenly, no scipy dependency."""
    order = np.argsort(values, kind="mergesort")
    ranks = np.empty(len(values), dtype=float)
    sorted_values = values[order]
    i = 0
    while i < len(values):
        j = i
        while j + 1 < len(values) and sorted_values[j + 1] == sorted_values[i]:
            j += 1
        avg_rank = (i + 1 + j + 1) / 2.0  # 1-indexed average over the tie block
        ranks[order[i : j + 1]] = avg_rank
        i = j + 1
    return ranks


def crps_gaussian(mu: np.ndarray, sigma: np.ndarray, y: np.ndarray) -> float:
    """Mean closed-form CRPS (Gneiting & Raftery) for a batch of Gaussian
    predictive distributions N(mu, sigma) evaluated at observed y. Verified
    against numerical integration of the CRPS definition (see gate 3.7's
    unit tests), not merely assumed correct."""
    mu = np.asarray(mu, dtype=float)
    sigma = np.asarray(sigma, dtype=float)
    y = np.asarray(y, dtype=float)
    z = (y - mu) / sigma
    phi = np.exp(-(z**2) / 2) / math.sqrt(2 * math.pi)
    cdf = 0.5 * (1 + np.vectorize(math.erf)(z / math.sqrt(2)))
    per_sample = sigma * (z * (2 * cdf - 1) + 2 * phi - 1 / math.sqrt(math.pi))
    return float(np.mean(per_sample))


def calibration_ratio(errors: np.ndarray, variances: np.ndarray) -> float:
    """mean(err^2) / mean(variance). 1.0 = perfectly calibrated; > 1 is
    underconfident (real errors exceed predicted variance); < 1 is
    overconfident. Identical formula to serving/monitoring/drift.py:138 in
    the MIRROR repo, ported here rather than cross-repo-imported."""
    errors = np.asarray(errors, dtype=float)
    variances = np.asarray(variances, dtype=float)
    mean_var = float(np.mean(variances))
    if mean_var == 0 or not math.isfinite(mean_var):
        return float("inf")
    return float(np.mean(errors**2)) / mean_var


@dataclass(frozen=True)
class HoldoutGateResult:
    auc: float
    crps: float
    calibration_ratio: float
    passed: bool
    failures: list[str] = field(default_factory=list)


def run_holdout_gate(
    *,
    labels: np.ndarray,
    scores: np.ndarray,
    predicted_mean: np.ndarray,
    predicted_sigma: np.ndarray,
    observed: np.ndarray,
    auc_min: float = 0.85,
    crps_max: float = 1.0,
    calibration_ratio_range: tuple[float, float] = (0.8, 1.2),
) -> HoldoutGateResult:
    """The full gate: a candidate must clear every threshold, not most of
    them. Any single failing axis fails the whole gate (invariant 1 in
    docs/phases/PHASE_3.md); this function never partially passes."""
    auc = roc_auc(labels, scores)
    errors = np.asarray(observed, dtype=float) - np.asarray(predicted_mean, dtype=float)
    variances = np.asarray(predicted_sigma, dtype=float) ** 2
    crps = crps_gaussian(predicted_mean, predicted_sigma, observed)
    ratio = calibration_ratio(errors, variances)

    failures: list[str] = []
    if auc < auc_min:
        failures.append(f"auc {auc:.4f} < auc_min {auc_min}")
    if crps > crps_max:
        failures.append(f"crps {crps:.4f} > crps_max {crps_max}")
    lo, hi = calibration_ratio_range
    if not (lo <= ratio <= hi):
        failures.append(f"calibration_ratio {ratio:.4f} outside [{lo}, {hi}]")

    return HoldoutGateResult(
        auc=auc, crps=crps, calibration_ratio=ratio, passed=not failures, failures=failures
    )
