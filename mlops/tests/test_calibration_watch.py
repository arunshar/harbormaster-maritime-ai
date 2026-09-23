"""Gate 4.2: mlops/calibration_watch.py."""

from __future__ import annotations

import pytest

from mlops.calibration_watch import DEFAULT_BAND, watch_calibration


def _pairs_with_ratio(ratio: float, n: int = 20) -> list[tuple[float, float]]:
    """n pairs whose calibration_ratio (mean(err^2)/mean(var)) is exactly ratio."""
    variance = 1.0
    error = ratio**0.5
    return [(error, variance)] * n


def test_in_band_ratio_returns_none_response():
    result = watch_calibration(_pairs_with_ratio(1.0))
    assert result.in_band is True
    assert result.response == "none"
    assert result.ratio == pytest.approx(1.0)


def test_out_of_band_ratio_triggers_supervised_retrain():
    result = watch_calibration(_pairs_with_ratio(1.5))
    assert result.in_band is False
    assert result.response == "supervised_retrain"


def test_boundary_values_are_in_band_inclusive():
    lo, hi = DEFAULT_BAND
    assert watch_calibration(_pairs_with_ratio(lo)).in_band is True
    assert watch_calibration(_pairs_with_ratio(hi)).in_band is True


def test_just_outside_boundary_is_out_of_band():
    lo, hi = DEFAULT_BAND
    assert watch_calibration(_pairs_with_ratio(lo - 0.01)).in_band is False
    assert watch_calibration(_pairs_with_ratio(hi + 0.01)).in_band is False


def test_insufficient_data_guard_returns_none_without_a_confident_verdict():
    # 3 labeled outcomes with a badly out-of-band ratio: the guard must still
    # refuse to trigger supervised_retrain until min_labeled is met.
    result = watch_calibration(_pairs_with_ratio(5.0, n=3), min_labeled=20)
    assert result.response == "none"
    assert result.n_labeled == 3
    assert "3" in result.reason


def test_custom_band_is_respected():
    result = watch_calibration(_pairs_with_ratio(1.3), band=(0.5, 1.5))
    assert result.in_band is True
