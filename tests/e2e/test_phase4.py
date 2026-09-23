"""Phase 4 acceptance (gate 4.7, the phase gate).

Like Phase 3 (not Phase 1/2), every Phase 4 gate's real logic lives in pure,
injectable functions, so all five criteria run unguarded in the ordinary
suite, no live stack or AWS needed. The AWS-only piece (gate 4.6's
drift_watch Terraform) is authored and plan-verified only, never applied
this sprint; criterion (e) checks its toggle hygiene structurally rather
than via a live `terraform plan`.

The five criteria map to docs/phases/PHASE_4.md's acceptance section:
  (a) input drift: PSI/KS port alerts on drifted data, silent on undrifted,
      matches MIRROR-computed expected values numerically
  (b) calibration drift: an out-of-band labeled stream returns
      supervised_retrain; boundary values match Phase 3's holdout-gate band
  (c) concept drift: rising disagreement routes to the preference pipeline
      and produces schema-valid triples; proxy 1 alone does not
  (d) reward hacking: a gamed candidate is blocked before shadow; an honest
      one passes; the pre-Phase-4 pinned promotion sequence is unchanged
      when no probe result is supplied
  (e) toggle hygiene: the drift_watch module is structurally gated on
      enable_phase4
"""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from e2e.phase4_helpers import ENV_MAIN_TF_PATH, drift_watch_module_is_gated_on_enable_phase4
from mlops.calibration_watch import watch_calibration
from mlops.concept_proxy import disagreement_rate, flag_uncertain_trace
from mlops.drift import check_input_drift
from mlops.drift_decision import classify_drift
from mlops.holdout_gate import HoldoutGateResult
from mlops.preference_builder import RewardBreakdown, build_from_hitl
from mlops.promote import run_promotion
from mlops.reward_hacking_probe import run_reward_hacking_probe
from mlops.shadow_diff import ShadowDiffResult

EXPECTATIONS = Path(__file__).parent.parent.parent / "mlops" / "fixtures" / "expectations.json"
HITL_THRESHOLD = 0.6
ROOT = Path(__file__).parent.parent.parent


def test_a_input_drift_alerts_on_drifted_and_matches_mirror_pinned_values():
    pinned = json.loads(EXPECTATIONS.read_text())["drift_check"]
    reference = pd.DataFrame(
        {"feature_a": np.linspace(0, 10, 50), "feature_b": np.linspace(0, 10, 50)}
    )
    current = pd.DataFrame(
        {"feature_a": np.linspace(0, 10, 50), "feature_b": np.linspace(5, 15, 50)}
    )
    results = {r.feature: r for r in check_input_drift(reference, current)}

    assert results["feature_a"].drifted == pinned["feature_a_drifted"]
    assert results["feature_b"].drifted == pinned["feature_b_drifted"]
    assert results["feature_b"].psi == pinned["feature_b_psi"]
    assert results["feature_b"].ks == pinned["feature_b_ks"]
    assert results["feature_b"].ks_pvalue == pytest.approx(pinned["feature_b_ks_pvalue"], rel=1e-12)


def test_b_out_of_band_calibration_triggers_supervised_retrain_at_the_phase3_band():
    variance = 1.0
    ratio = 1.5
    pairs = [(ratio**0.5, variance)] * 30  # calibration_ratio == 1.5, outside [0.8, 1.2]
    result = watch_calibration(pairs)
    assert result.response == "supervised_retrain"

    # exact boundary values match the Phase 3 holdout gate's [0.8, 1.2] band
    assert watch_calibration([((0.8) ** 0.5, 1.0)] * 30).in_band is True
    assert watch_calibration([((1.2) ** 0.5, 1.0)] * 30).in_band is True
    assert watch_calibration([((0.79) ** 0.5, 1.0)] * 30).in_band is False


def test_c_rising_disagreement_routes_to_preference_pipeline_with_valid_triples():
    hitl_rows = [
        {
            "trace_id": f"t{i}",
            "mmsi": 200000000 + i,
            "score": 0.9,
            "label": "incorrect" if i % 2 == 0 else "correct",
            "reviewer": "bob",
        }
        for i in range(20)
    ]
    disagreement = disagreement_rate(hitl_rows, hitl_threshold=HITL_THRESHOLD)
    stable = pd.DataFrame({"a": np.linspace(0, 1, 20)})
    in_band = watch_calibration([(1.0, 1.0)] * 30)

    decision = classify_drift(
        input_results=check_input_drift(stable, stable),
        calibration=in_band,
        disagreement=disagreement,
    )
    assert decision.category == "concept_drift"
    assert decision.response == "preference_pipeline"

    triples = build_from_hitl(hitl_rows, contexts={}, hitl_threshold=HITL_THRESHOLD)
    assert len(triples) == 10  # every "incorrect" row
    for t in triples:
        assert t.preference_source == "hitl_verdict"
        assert isinstance(t.structural_violation_in_either_arm, bool)

    # proxy 1 alone (elevated, but disagreement flat) must NOT trigger concept drift
    proxy1_flags = [
        flag_uncertain_trace(0.6, 0.5, hitl_threshold=HITL_THRESHOLD) for _ in range(10)
    ]
    assert all(proxy1_flags)
    flat_disagreement = disagreement_rate(
        [{"score": 0.9, "label": "correct"}] * 20, hitl_threshold=HITL_THRESHOLD
    )
    no_drift_decision = classify_drift(
        input_results=check_input_drift(stable, stable),
        calibration=in_band,
        disagreement=flat_disagreement,
    )
    assert no_drift_decision.category != "concept_drift"


def test_d_gamed_candidate_blocked_before_shadow_honest_candidate_passes():
    baseline = [
        RewardBreakdown(total=5.0, structural=0.5, shaping=1.0, data=1.0, pref=1.0)
        for _ in range(10)
    ]
    gamed = [
        RewardBreakdown(total=8.0, structural=-1.0, shaping=3.0, data=3.0, pref=3.0)
        for _ in range(10)
    ]
    honest = [
        RewardBreakdown(total=8.0, structural=0.5, shaping=1.5, data=1.5, pref=1.5)
        for _ in range(10)
    ]

    passing_gate = HoldoutGateResult(
        auc=0.95, crps=0.2, calibration_ratio=1.0, passed=True, failures=[]
    )
    clean_shadow = ShadowDiffResult(
        mean_abs_diff=0.01, max_abs_diff=0.02, n_samples=100, passed=True
    )

    gamed_probe = run_reward_hacking_probe(baseline, gamed)
    gamed_promotion = run_promotion(
        holdout_result=passing_gate,
        shadow_result=clean_shadow,
        burn_check=lambda w: False,
        set_canary_weight=lambda w: None,
        revert_to_champion=lambda: None,
        reward_hacking_result=gamed_probe,
    )
    assert gamed_promotion.final_status == "rejected_reward_probe"
    assert [s.stage for s in gamed_promotion.steps] == ["gate", "reward_probe"]

    honest_probe = run_reward_hacking_probe(baseline, honest)
    honest_promotion = run_promotion(
        holdout_result=passing_gate,
        shadow_result=clean_shadow,
        burn_check=lambda w: False,
        set_canary_weight=lambda w: None,
        revert_to_champion=lambda: None,
        reward_hacking_result=honest_probe,
    )
    assert honest_promotion.final_status == "promoted"

    # None (a supervised-retrain candidate) reproduces the pre-Phase-4 pinned
    # sequence byte-for-byte -- Phase 4 changed nothing about that path.
    none_probe_promotion = run_promotion(
        holdout_result=passing_gate,
        shadow_result=clean_shadow,
        burn_check=lambda w: False,
        set_canary_weight=lambda w: None,
        revert_to_champion=lambda: None,
        reward_hacking_result=None,
    )
    pinned = json.loads(EXPECTATIONS.read_text())["promotion_state_machine"]["clean_run"]
    assert [asdict(s) for s in none_probe_promotion.steps] == pinned


def test_e_drift_watch_module_is_structurally_gated_on_enable_phase4():
    assert drift_watch_module_is_gated_on_enable_phase4(ENV_MAIN_TF_PATH.read_text()) is True


def test_f_drift_watch_package_target_matches_the_declared_lambda_architecture():
    """A macOS-native numpy/pyarrow package would plan successfully but fail
    inside the Linux Lambda runtime. Keep the package target and deployed
    architecture explicit and paired."""
    makefile = (ROOT / "Makefile").read_text()
    module = (ROOT / "infra" / "terraform" / "modules" / "drift_watch" / "main.tf").read_text()

    assert "DRIFT_LAMBDA_PLATFORM := manylinux2014_x86_64" in makefile
    assert "--platform $(DRIFT_LAMBDA_PLATFORM)" in makefile
    assert "--only-binary=:all:" in makefile
    assert "DRIFT_LAMBDA_MAX_UNZIPPED_KB := 256000" in makefile
    assert "-name test -o -name tests -o -name __pycache__" in makefile
    assert 'architectures    = ["x86_64"]' in module
    assert 'resource "aws_s3_object" "drift_watch_package"' in module
    assert "s3_bucket        = var.lambda_artifacts_bucket_name" in module
    assert "source_hash = data.archive_file.drift_watch.output_base64sha256" in module


def test_g_snapshot_refresh_has_an_immutable_window_then_current_pointer_boundary():
    """Keep the Phase 4 publisher's reference protection and package input explicit.

    The detailed ordering and invalid-source behavior are exercised by the
    focused in-memory S3 tests. This source-contract test makes a later
    Terraform or package refactor fail before it can remove the role boundary.
    """
    makefile = (ROOT / "Makefile").read_text()
    module = (ROOT / "infra" / "terraform" / "modules" / "drift_watch" / "main.tf").read_text()
    refresh = (ROOT / "infra" / "lambda" / "drift_watch" / "snapshot_refresh.py").read_text()

    assert "snapshot_refresh.py" in makefile
    assert "ListRawAisCandidatesAndValidatedWindows" in module
    assert "PublishValidatedWindowAndAdvanceCurrentOnly" in module
    assert '"s3:PutObject"' in module
    assert "snapshot_window_prefix" in module
    assert "reference_snapshot_key and every raw object" in module
    assert "_put_immutable(" in refresh
    assert 'IfNoneMatch="*"' in refresh
    assert "_current_matches_window(" in refresh
    assert "copy_object(" in refresh
    assert "reference and current snapshot keys must differ" in refresh
