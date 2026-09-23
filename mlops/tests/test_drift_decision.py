"""Gate 4.3: mlops/drift_decision.py, the decision-table truth table.

Every row of docs/phases/PHASE_4_SKETCH.md's table, plus the explicit
false-alarm cross-validation row (proxy 1 rising alone is NOT concept
drift).
"""

from __future__ import annotations

from mlops.calibration_watch import CalibrationWatchResult
from mlops.concept_proxy import DisagreementResult
from mlops.drift import DriftResult
from mlops.drift_decision import DISAGREEMENT_ALERT_RATE_PLACEHOLDER, classify_drift

IN_BAND_CALIBRATION = CalibrationWatchResult(
    ratio=1.0, band=(0.8, 1.2), in_band=True, n_labeled=50, response="none", reason="in band"
)
OUT_OF_BAND_CALIBRATION = CalibrationWatchResult(
    ratio=1.5,
    band=(0.8, 1.2),
    in_band=False,
    n_labeled=50,
    response="supervised_retrain",
    reason="out",
)
NO_INPUT_DRIFT = [DriftResult(feature="a", psi=0.0, ks=0.0, ks_pvalue=1.0, drifted=False)]
INPUT_DRIFT = [DriftResult(feature="a", psi=0.5, ks=0.4, ks_pvalue=0.001, drifted=True)]
FLAT_DISAGREEMENT = DisagreementResult(rate=0.05, n=50, excluded=0)
RISING_DISAGREEMENT = DisagreementResult(
    rate=DISAGREEMENT_ALERT_RATE_PLACEHOLDER + 0.1, n=50, excluded=0
)
NO_DISAGREEMENT_DATA = DisagreementResult(rate=0.0, n=0, excluded=0)


def test_no_drift_at_all_is_none_log_only():
    decision = classify_drift(
        input_results=NO_INPUT_DRIFT,
        calibration=IN_BAND_CALIBRATION,
        disagreement=FLAT_DISAGREEMENT,
    )
    assert decision.category == "none"
    assert decision.response == "log_only"


def test_input_drift_only_is_log_only_not_retrain():
    decision = classify_drift(
        input_results=INPUT_DRIFT, calibration=IN_BAND_CALIBRATION, disagreement=FLAT_DISAGREEMENT
    )
    assert decision.category == "input_drift"
    assert decision.response == "log_only"


def test_calibration_drift_triggers_supervised_retrain():
    decision = classify_drift(
        input_results=NO_INPUT_DRIFT,
        calibration=OUT_OF_BAND_CALIBRATION,
        disagreement=FLAT_DISAGREEMENT,
    )
    assert decision.category == "calibration_drift"
    assert decision.response == "supervised_retrain"


def test_rising_disagreement_triggers_preference_pipeline_regardless_of_calibration():
    decision = classify_drift(
        input_results=NO_INPUT_DRIFT,
        calibration=IN_BAND_CALIBRATION,
        disagreement=RISING_DISAGREEMENT,
    )
    assert decision.category == "concept_drift"
    assert decision.response == "preference_pipeline"


def test_rising_disagreement_overrides_calibration_drift_too():
    decision = classify_drift(
        input_results=NO_INPUT_DRIFT,
        calibration=OUT_OF_BAND_CALIBRATION,
        disagreement=RISING_DISAGREEMENT,
    )
    assert decision.category == "concept_drift"


def test_proxy_1_rising_alone_with_flat_disagreement_is_not_concept_drift():
    # The sketch's explicit false-alarm row: input/proxy-1 signal present,
    # disagreement (proxy 2) flat -> "more ambiguous traffic, still right
    # about it", not concept drift.
    decision = classify_drift(
        input_results=INPUT_DRIFT, calibration=IN_BAND_CALIBRATION, disagreement=FLAT_DISAGREEMENT
    )
    assert decision.category != "concept_drift"


def test_zero_labeled_disagreement_data_never_triggers_concept_drift():
    decision = classify_drift(
        input_results=NO_INPUT_DRIFT,
        calibration=IN_BAND_CALIBRATION,
        disagreement=NO_DISAGREEMENT_DATA,
    )
    assert decision.category != "concept_drift"


def test_custom_disagreement_alert_rate_is_respected():
    borderline = DisagreementResult(rate=0.15, n=50, excluded=0)
    decision = classify_drift(
        input_results=NO_INPUT_DRIFT,
        calibration=IN_BAND_CALIBRATION,
        disagreement=borderline,
        disagreement_alert_rate=0.1,
    )
    assert decision.category == "concept_drift"
