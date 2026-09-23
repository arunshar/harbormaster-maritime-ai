"""Gate 4.3: mlops/disagreement_baseline.py, the alert-threshold derivation.

Every expected threshold is hand-computed in a comment next to its
assertion. Window rates are exact binary fractions (0.03125 = 1/32,
0.0625 = 1/16, 0.25 = 1/4, 0.5 = 1/2) so each hand-computed value is an
exact float equality, not an approximation.
"""

from __future__ import annotations

import math

import pytest

from mlops.calibration_watch import CalibrationWatchResult
from mlops.concept_proxy import DisagreementResult
from mlops.disagreement_baseline import (
    BaselineWindow,
    DisagreementBaseline,
    derive_alert_threshold,
    make_disagreement_alert_rate,
)
from mlops.drift import DriftResult
from mlops.drift_decision import DISAGREEMENT_ALERT_RATE_PLACEHOLDER, classify_drift

IN_BAND_CALIBRATION = CalibrationWatchResult(
    ratio=1.0, band=(0.8, 1.2), in_band=True, n_labeled=50, response="none", reason="in band"
)
NO_INPUT_DRIFT = [DriftResult(feature="a", psi=0.0, ks=0.0, ks_pvalue=1.0, drifted=False)]


def baseline_of(*windows: BaselineWindow) -> DisagreementBaseline:
    baseline = DisagreementBaseline()
    for window in windows:
        baseline.append(window)
    return baseline


def test_append_rejects_rate_outside_the_unit_interval():
    baseline = DisagreementBaseline()
    with pytest.raises(ValueError):
        baseline.append(BaselineWindow(rate=-0.1, n=50))
    with pytest.raises(ValueError):
        baseline.append(BaselineWindow(rate=1.1, n=50))


def test_append_rejects_negative_n():
    with pytest.raises(ValueError):
        DisagreementBaseline().append(BaselineWindow(rate=0.1, n=-1))


def test_usable_windows_excludes_tiny_n_windows():
    # A rate over 3 rows is quantized in steps of 1/3; it is kept in the
    # record but excluded from derivation (default min_window_n = 20).
    baseline = baseline_of(BaselineWindow(rate=0.0625, n=50), BaselineWindow(rate=1.0, n=3))
    assert len(baseline.windows) == 2
    assert baseline.usable_windows() == [BaselineWindow(rate=0.0625, n=50)]


def test_usable_window_minimum_is_inclusive_at_20_labels():
    below = BaselineWindow(rate=0.25, n=19)
    at = BaselineWindow(rate=0.5, n=20)
    baseline = baseline_of(below, at)

    assert baseline.usable_windows(min_window_n=20) == [at]


def test_derive_threshold_q95_dominates_on_a_heavy_tail():
    # Rates sorted: [1/32, 1/32, 1/32, 1/32, 1/4].
    # q95 nearest rank: ceil(0.95 * 5) = 5, so the 5th smallest = 0.25.
    # mean = (4 * 0.03125 + 0.25) / 5 = 0.375 / 5 = 0.075; 2 * mean = 0.15.
    # floor = 0.05. max(0.25, 0.15, 0.05) = 0.25.
    baseline = baseline_of(
        *(BaselineWindow(rate=0.03125, n=50) for _ in range(4)),
        BaselineWindow(rate=0.25, n=50),
    )
    assert derive_alert_threshold(baseline) == 0.25


def test_derive_threshold_mean_guard_dominates_on_a_flat_baseline():
    # Five windows all at 1/16: q95 = 0.0625, mean = 0.3125 / 5 = 0.0625,
    # 2 * mean = 0.125, floor = 0.05. max = 0.125 (every value binary exact).
    baseline = baseline_of(*(BaselineWindow(rate=0.0625, n=50) for _ in range(5)))
    assert derive_alert_threshold(baseline) == 0.125


def test_derive_threshold_floor_dominates_on_a_quiet_baseline():
    # Five windows with zero disagreements: q95 = 0.0, 2 * mean = 0.0, so
    # the floor 0.05 (one override in a 20-row window) is the threshold.
    baseline = baseline_of(*(BaselineWindow(rate=0.0, n=50) for _ in range(5)))
    assert derive_alert_threshold(baseline) == 0.05


def test_derive_threshold_ignores_tiny_n_windows():
    # The junk window (rate 1.0 over 3 rows) is excluded by min_window_n,
    # so the threshold is the flat-baseline 0.125 from the five usable
    # windows (hand computation above), not dragged up to 1.0.
    baseline = baseline_of(
        *(BaselineWindow(rate=0.0625, n=50) for _ in range(5)),
        BaselineWindow(rate=1.0, n=3),
    )
    assert derive_alert_threshold(baseline) == 0.125


def test_derive_threshold_refuses_below_min_windows():
    baseline = baseline_of(*(BaselineWindow(rate=0.0625, n=50) for _ in range(4)))
    with pytest.raises(ValueError):
        derive_alert_threshold(baseline)


def test_q95_nearest_rank_at_21_windows_is_the_20th_smallest():
    # ceil(0.95 * 21) = ceil(19.95) = 20: with 19 windows at 1/32 and the
    # top two at [1/4, 1/2] sorted, the 20th smallest is 0.25, not the max.
    # mean = (19 / 32 + 1 / 4 + 1 / 2) / 21 = 1.34375 / 21, about 0.064;
    # 2 * mean about 0.128 < 0.25. max = 0.25.
    baseline = baseline_of(
        *(BaselineWindow(rate=0.03125, n=50) for _ in range(19)),
        BaselineWindow(rate=0.25, n=50),
        BaselineWindow(rate=0.5, n=50),
    )
    assert derive_alert_threshold(baseline) == 0.25


@pytest.mark.parametrize(
    ("rates", "expected"),
    [
        ([0.0] * 18 + [0.5], 0.5),
        ([0.0] * 18 + [0.25, 0.5], 0.25),
    ],
)
def test_q95_nearest_rank_boundary_at_19_and_20_windows(rates, expected):
    # k=19: rank 19 selects the maximum 0.5. k=20: rank 19 selects the
    # second maximum 0.25. In both fixtures q95 dominates 2*mean and 0.05.
    baseline = baseline_of(*(BaselineWindow(rate=rate, n=50) for rate in rates))
    assert derive_alert_threshold(baseline) == expected


def test_make_disagreement_alert_rate_falls_back_below_min_windows():
    baseline = baseline_of(*(BaselineWindow(rate=0.0625, n=50) for _ in range(4)))
    assert make_disagreement_alert_rate(baseline) == DISAGREEMENT_ALERT_RATE_PLACEHOLDER


def test_make_disagreement_alert_rate_falls_back_when_all_windows_are_tiny():
    baseline = baseline_of(*(BaselineWindow(rate=0.0625, n=5) for _ in range(10)))
    assert make_disagreement_alert_rate(baseline) == DISAGREEMENT_ALERT_RATE_PLACEHOLDER


def test_make_disagreement_alert_rate_derives_once_enough_windows_exist():
    baseline = baseline_of(*(BaselineWindow(rate=0.0625, n=50) for _ in range(5)))
    assert make_disagreement_alert_rate(baseline) == 0.125


def test_derived_threshold_wires_into_classify_drift_and_flips_at_the_boundary():
    # The flat baseline of five 1/16 windows derives threshold 0.125 (hand
    # computation in test_derive_threshold_mean_guard_dominates...).
    # classify_drift alerts on rate >= threshold, so the decision must flip
    # between the largest float below 0.125 and 0.125 itself.
    baseline = baseline_of(*(BaselineWindow(rate=0.0625, n=50) for _ in range(5)))
    threshold = make_disagreement_alert_rate(baseline)
    assert threshold == 0.125

    just_below = classify_drift(
        input_results=NO_INPUT_DRIFT,
        calibration=IN_BAND_CALIBRATION,
        disagreement=DisagreementResult(rate=math.nextafter(threshold, 0.0), n=50, excluded=0),
        disagreement_alert_rate=threshold,
    )
    at_boundary = classify_drift(
        input_results=NO_INPUT_DRIFT,
        calibration=IN_BAND_CALIBRATION,
        disagreement=DisagreementResult(rate=threshold, n=50, excluded=0),
        disagreement_alert_rate=threshold,
    )
    assert just_below.category == "none"
    assert at_boundary.category == "concept_drift"
    assert at_boundary.response == "preference_pipeline"
