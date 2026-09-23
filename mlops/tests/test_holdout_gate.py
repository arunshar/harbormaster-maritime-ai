"""Holdout gate tests (Phase 3, gate 3.7). Each metric is verified against
an independent reference before being trusted in the gate itself: roc_auc
against sklearn.metrics.roc_auc_score (see the commit note; not re-checked
here to avoid a scikit-learn test dependency for the mlops package), and
crps_gaussian against numerical integration of the CRPS definition.
"""

from __future__ import annotations

import math

import numpy as np
import pytest

from mlops.holdout_gate import (
    HoldoutGateResult,
    calibration_ratio,
    crps_gaussian,
    roc_auc,
    run_holdout_gate,
)


def test_roc_auc_perfect_separation_is_one():
    labels = np.array([0, 0, 0, 1, 1, 1])
    scores = np.array([0.1, 0.2, 0.3, 0.7, 0.8, 0.9])
    assert roc_auc(labels, scores) == 1.0


def test_roc_auc_inverted_separation_is_zero():
    labels = np.array([0, 0, 0, 1, 1, 1])
    scores = np.array([0.9, 0.8, 0.7, 0.3, 0.2, 0.1])
    assert roc_auc(labels, scores) == 0.0


def test_roc_auc_ties_split_evenly():
    # one tied pair (a positive and a negative share a score): matches the
    # hand-verified value cross-checked against sklearn.metrics.roc_auc_score
    labels = np.array([0, 1, 0, 1])
    scores = np.array([0.5, 0.5, 0.3, 0.7])
    assert roc_auc(labels, scores) == pytest.approx(0.875)


def test_roc_auc_requires_both_classes_present():
    with pytest.raises(ValueError):
        roc_auc(np.array([1, 1, 1]), np.array([0.1, 0.2, 0.3]))


@pytest.mark.parametrize(
    "mu,sigma,y,expected",
    [
        (0.0, 1.0, 0.0, 0.233695),
        (0.0, 1.0, 1.0, 0.602441),
        (2.0, 0.5, 2.3, 0.186578),
        (5.0, 2.0, 1.0, 2.905584),
    ],
)
def test_crps_gaussian_matches_numerically_integrated_reference_values(mu, sigma, y, expected):
    # reference values computed by numerically integrating the CRPS
    # definition integral (F(x) - 1{x>=y})^2 dx with scipy.integrate.quad,
    # independent of this module's closed-form implementation
    result = crps_gaussian(np.array([mu]), np.array([sigma]), np.array([y]))
    assert result == pytest.approx(expected, abs=1e-5)


def test_crps_gaussian_averages_over_a_batch():
    mu = np.array([0.0, 0.0])
    sigma = np.array([1.0, 1.0])
    y = np.array([0.0, 1.0])
    expected_mean = (0.233695 + 0.602441) / 2
    assert crps_gaussian(mu, sigma, y) == pytest.approx(expected_mean, abs=1e-5)


def test_calibration_ratio_is_one_when_errors_match_variance():
    errors = np.array([1.0, -1.0, 2.0, -2.0])
    # a variance that exactly matches mean squared error gives a clean ratio of 1.0
    mse = float(np.mean(errors**2))
    matched_variances = np.full(4, mse)
    assert calibration_ratio(errors, matched_variances) == pytest.approx(1.0)


def test_calibration_ratio_above_one_is_underconfident():
    errors = np.array([3.0, -3.0])
    variances = np.array([1.0, 1.0])  # real errors far exceed predicted variance
    assert calibration_ratio(errors, variances) > 1.0


def test_calibration_ratio_below_one_is_overconfident():
    errors = np.array([0.1, -0.1])
    variances = np.array([1.0, 1.0])  # real errors much smaller than predicted variance
    assert calibration_ratio(errors, variances) < 1.0


def test_calibration_ratio_handles_zero_variance_as_infinite():
    assert math.isinf(calibration_ratio(np.array([1.0]), np.array([0.0])))


def _clean_gate_inputs(rng, n=200):
    labels = rng.integers(0, 2, size=n)
    scores = labels * 0.9 + rng.normal(0, 0.1, size=n)  # cleanly separable -> high AUC
    predicted_mean = rng.normal(0, 1, size=n)
    predicted_sigma = np.full(n, 1.0)
    observed = predicted_mean + rng.normal(0, 1, size=n)  # errors match the predicted sigma
    return labels, scores, predicted_mean, predicted_sigma, observed


def test_run_holdout_gate_passes_a_clean_candidate():
    rng = np.random.default_rng(0)
    labels, scores, mean, sigma, observed = _clean_gate_inputs(rng)
    result = run_holdout_gate(
        labels=labels, scores=scores, predicted_mean=mean, predicted_sigma=sigma, observed=observed
    )
    assert isinstance(result, HoldoutGateResult)
    assert result.passed, result.failures


def test_run_holdout_gate_fails_on_low_auc_alone():
    # a large n tightens calibration_ratio's sampling variance around its true
    # value of 1.0, so this isolates the AUC failure without also drifting
    # calibration_ratio outside [0.8, 1.2] by chance
    rng = np.random.default_rng(1)
    n = 20_000
    labels = rng.integers(0, 2, size=n)
    scores = rng.normal(0, 1, size=n)  # uninformative: AUC ~ 0.5
    mean = rng.normal(0, 1, size=n)
    sigma = np.full(n, 1.0)
    observed = mean + rng.normal(0, 1, size=n)
    result = run_holdout_gate(
        labels=labels, scores=scores, predicted_mean=mean, predicted_sigma=sigma, observed=observed
    )
    assert not result.passed
    assert any("auc" in f for f in result.failures)
    assert not any("crps" in f for f in result.failures)
    assert not any("calibration_ratio" in f for f in result.failures)


def test_run_holdout_gate_fails_on_high_crps_alone():
    rng = np.random.default_rng(2)
    n = 200
    labels, scores, _, _, _ = _clean_gate_inputs(rng, n)
    mean = np.zeros(n)
    sigma = np.full(n, 1.0)
    observed = np.full(n, 50.0)  # wildly off predictions -> large CRPS
    result = run_holdout_gate(
        labels=labels, scores=scores, predicted_mean=mean, predicted_sigma=sigma, observed=observed
    )
    assert not result.passed
    assert any("crps" in f for f in result.failures)


def test_run_holdout_gate_fails_on_bad_calibration_alone():
    rng = np.random.default_rng(3)
    n = 200
    labels, scores, _, _, _ = _clean_gate_inputs(rng, n)
    mean = np.zeros(n)
    sigma = np.full(n, 0.01)  # wildly overconfident
    observed = rng.normal(0, 1, size=n)  # real errors far exceed the tiny predicted sigma
    result = run_holdout_gate(
        labels=labels,
        scores=scores,
        predicted_mean=mean,
        predicted_sigma=sigma,
        observed=observed,
        crps_max=100.0,  # isolate the calibration failure from a CRPS failure
    )
    assert not result.passed
    assert any("calibration_ratio" in f for f in result.failures)


@pytest.mark.parametrize(
    "ratio_bound,should_pass", [(0.79, False), (0.8, True), (1.2, True), (1.21, False)]
)
def test_calibration_ratio_range_boundaries(ratio_bound, should_pass):
    # construct errors/variance so calibration_ratio comes out to exactly ratio_bound
    variance = 1.0
    errors = np.array([math.sqrt(ratio_bound * variance)])
    ratio = calibration_ratio(errors, np.array([variance]))
    assert ratio == pytest.approx(ratio_bound)

    rng = np.random.default_rng(4)
    labels, scores, _, _, _ = _clean_gate_inputs(rng)
    n = len(labels)
    result = run_holdout_gate(
        labels=labels,
        scores=scores,
        predicted_mean=np.zeros(n),
        predicted_sigma=np.full(n, math.sqrt(variance)),
        observed=np.full(n, math.sqrt(ratio_bound * variance)),
        crps_max=1e9,  # isolate the calibration-range check
    )
    assert result.passed is should_pass
