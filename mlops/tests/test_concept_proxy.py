"""Gate 4.3: mlops/concept_proxy.py, the two concept-drift proxies."""

from __future__ import annotations

from mlops.concept_proxy import disagreement_rate, flag_uncertain_trace

THRESHOLD = 0.6


def test_flag_uncertain_trace_fires_near_threshold_and_high_variance():
    assert flag_uncertain_trace(0.61, 0.2, hitl_threshold=THRESHOLD) is True


def test_flag_uncertain_trace_does_not_fire_far_from_threshold():
    assert flag_uncertain_trace(0.95, 0.5, hitl_threshold=THRESHOLD) is False


def test_flag_uncertain_trace_does_not_fire_on_low_variance():
    assert flag_uncertain_trace(0.6, 0.01, hitl_threshold=THRESHOLD) is False


def test_flag_uncertain_trace_never_fires_when_variance_is_none():
    # no signal is not "high uncertainty"
    assert flag_uncertain_trace(0.6, None, hitl_threshold=THRESHOLD) is False


def test_disagreement_rate_counts_incorrect_over_labeled_rows():
    rows = [
        {"score": 0.9, "label": "correct"},
        {"score": 0.9, "label": "incorrect"},
        {"score": 0.9, "label": "incorrect"},
        {"score": 0.9, "label": "correct"},
    ]
    result = disagreement_rate(rows, hitl_threshold=THRESHOLD)
    assert result.n == 4
    assert result.rate == 0.5
    assert result.excluded == 0


def test_disagreement_rate_excludes_ambiguous_and_unlabeled():
    rows = [
        {"score": 0.9, "label": "correct"},
        {"score": 0.9, "label": "ambiguous"},
        {"score": 0.9, "label": None},
    ]
    result = disagreement_rate(rows, hitl_threshold=THRESHOLD)
    assert result.n == 1
    assert result.excluded == 2


def test_disagreement_rate_excludes_rows_below_the_model_implied_threshold():
    rows = [
        {"score": 0.9, "label": "correct"},
        {"score": 0.1, "label": "incorrect"},  # below threshold: data inconsistency, excluded
    ]
    result = disagreement_rate(rows, hitl_threshold=THRESHOLD)
    assert result.n == 1
    assert result.excluded == 1


def test_disagreement_rate_zero_labeled_rows_returns_zero_not_a_crash():
    result = disagreement_rate([], hitl_threshold=THRESHOLD)
    assert result.n == 0
    assert result.rate == 0.0
