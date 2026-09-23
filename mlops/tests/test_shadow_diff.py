from __future__ import annotations

import numpy as np
import pytest

from mlops.shadow_diff import score_diff


def test_clean_pair_passes():
    champion = np.array([0.1, 0.5, 0.9, 0.3])
    shadow = np.array([0.11, 0.49, 0.91, 0.31])  # tiny divergence
    result = score_diff(champion, shadow, max_divergence=0.05)
    assert result.passed
    assert result.n_samples == 4


def test_diverging_pair_fails():
    champion = np.array([0.1, 0.5, 0.9, 0.3])
    shadow = np.array([0.9, 0.1, 0.1, 0.9])  # large, systematic divergence
    result = score_diff(champion, shadow, max_divergence=0.05)
    assert not result.passed


def test_mismatched_lengths_raise():
    with pytest.raises(ValueError):
        score_diff(np.array([0.1, 0.2]), np.array([0.1]), max_divergence=0.1)


def test_empty_arrays_raise():
    with pytest.raises(ValueError):
        score_diff(np.array([]), np.array([]), max_divergence=0.1)


def test_max_abs_diff_is_reported_even_when_mean_passes():
    # one outlier pair: mean stays low, but max_abs_diff surfaces the outlier
    # for a human reviewing the shadow window, not just a pass/fail bit
    champion = np.array([0.1, 0.1, 0.1, 0.1, 0.9])
    shadow = np.array([0.1, 0.1, 0.1, 0.1, 0.1])
    result = score_diff(champion, shadow, max_divergence=0.2)
    assert result.passed
    assert result.max_abs_diff == pytest.approx(0.8)
