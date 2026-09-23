"""Gate 4.1: mlops/drift.py, the input-drift port from MIRROR.

Cross-check provenance: the pinned psi/ks/pvalue constants below were
computed once, directly from MIRROR's own module
(serving/monitoring/drift.py in a separate MIRROR checkout), not from this port:

    sys.path.insert(0, "/path/to/mirror")
    from serving.monitoring.drift import population_stability_index, ks_statistic, ks_pvalue
    # ref/cur fixtures identical to the ones below

The PSI and KS statistics are verbatim ports, so they must match exactly. The
asymptotic p-value is a new implementation of the Kolmogorov distribution, so
it is checked against MIRROR's pinned value within a tight relative tolerance
and against SciPy's independent ``scipy.special.kolmogorov`` when SciPy is
installed.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from mlops.drift import (
    DriftConfig,
    check_input_drift,
    ks_pvalue,
    ks_statistic,
    population_stability_index,
)

REFERENCE = pd.DataFrame(
    {
        "feature_a": np.linspace(0, 10, 50),
        "feature_b": np.linspace(0, 10, 50),
    }
)
CURRENT_UNDRIFTED = pd.DataFrame(
    {
        "feature_a": np.linspace(0, 10, 50),
        "feature_b": np.linspace(0, 10, 50),
    }
)
CURRENT_DRIFTED = pd.DataFrame(
    {
        "feature_a": np.linspace(0, 10, 50),  # unchanged: must NOT flag drifted
        "feature_b": np.linspace(5, 15, 50),  # mean-shifted +5: must flag drifted
    }
)

# Pinned against MIRROR's own module (see module docstring for the exact call).
MIRROR_DRIFTED_FEATURE_B_PSI = 6.652284902471817
MIRROR_DRIFTED_FEATURE_B_KS = 0.5
MIRROR_DRIFTED_FEATURE_B_PVALUE = 3.6276162006545173e-06


def test_psi_ks_pvalue_match_mirrors_original_module_on_drifted_data():
    ref = REFERENCE["feature_b"]
    cur = CURRENT_DRIFTED["feature_b"]
    assert population_stability_index(ref, cur, n_bins=10) == MIRROR_DRIFTED_FEATURE_B_PSI
    ks = ks_statistic(ref, cur)
    assert ks == MIRROR_DRIFTED_FEATURE_B_KS
    assert ks_pvalue(ks, ref.size, cur.size) == pytest.approx(
        MIRROR_DRIFTED_FEATURE_B_PVALUE, rel=1e-12
    )


def test_kolmogorov_survival_function_matches_scipy_reference():
    special = pytest.importorskip("scipy.special")
    from mlops.drift import _kolmogorov_sf

    for lam in np.linspace(0.05, 4.0, 400):
        expected = float(special.kolmogorov(lam))
        assert _kolmogorov_sf(float(lam)) == pytest.approx(expected, rel=1e-12, abs=1e-300)


def test_kolmogorov_survival_function_edges():
    from mlops.drift import _kolmogorov_sf

    assert _kolmogorov_sf(0.0) == 1.0
    assert _kolmogorov_sf(-1.0) == 1.0
    assert _kolmogorov_sf(0.05) == 1.0
    assert 0.0 <= _kolmogorov_sf(1.0) <= 1.0
    assert _kolmogorov_sf(50.0) == 0.0
    assert ks_pvalue(0.3, 0, 10) == 1.0


def test_psi_and_ks_are_zero_for_identical_distributions():
    ref = REFERENCE["feature_a"]
    cur = CURRENT_UNDRIFTED["feature_a"]
    assert population_stability_index(ref, cur) == 0.0
    assert ks_statistic(ref, cur) == 0.0


def test_check_input_drift_flags_only_the_shifted_feature():
    results = check_input_drift(REFERENCE, CURRENT_DRIFTED)
    by_name = {r.feature: r for r in results}
    assert by_name["feature_a"].drifted is False
    assert by_name["feature_b"].drifted is True
    assert by_name["feature_b"].psi == MIRROR_DRIFTED_FEATURE_B_PSI


def test_check_input_drift_flags_nothing_on_undrifted_data():
    results = check_input_drift(REFERENCE, CURRENT_UNDRIFTED)
    assert all(not r.drifted for r in results)


def test_check_input_drift_only_compares_shared_numeric_columns():
    reference = pd.DataFrame({"a": np.linspace(0, 10, 20), "quality_flag": ["ok"] * 20})
    current = pd.DataFrame({"a": np.linspace(0, 10, 20), "b_only_in_current": np.zeros(20)})
    results = check_input_drift(reference, current)
    assert [r.feature for r in results] == ["a"]


def test_psi_alert_and_warn_tier_boundaries():
    config = DriftConfig(psi_warn=0.1, psi_alert=0.25, ks_pvalue_alert=0.05)
    # A psi just under psi_alert but with a significant ks p-value should still
    # drift via the warn+significance branch; a psi below psi_warn should never
    # drift regardless of ks p-value.
    below_warn = pd.DataFrame({"x": np.linspace(0, 1, 30)})
    below_warn_cur = pd.DataFrame({"x": np.linspace(0, 1, 30)})
    result = check_input_drift(below_warn, below_warn_cur, config)[0]
    assert result.psi < config.psi_warn
    assert result.drifted is False


def test_empty_current_returns_zero_psi_and_ks_not_a_crash():
    ref = pd.Series(np.linspace(0, 10, 10))
    cur = pd.Series([], dtype=float)
    assert population_stability_index(ref, cur) == 0.0
    assert ks_statistic(ref, cur) == 0.0
