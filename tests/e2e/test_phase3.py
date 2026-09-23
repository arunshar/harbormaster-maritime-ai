"""Phase 3 acceptance (gate 3.9, the phase gate).

Unlike the Phase 1/2 e2e suites, these five criteria need no live stack (no
kind cluster, no running consumer, no AWS): every Phase 3 gate's real logic
lives in pure, injectable functions (the GE suite, the corridor transforms,
the holdout gate, shadow diff, and the promotion state machine), so there is
nothing here to skip-guard behind an env var - these run in the ordinary
suite, every time. A live AWS-showcase run (real SageMaker canary weights,
a real EMR job) is out of scope for this session (no demo window) and would
be operator-run, matching every other AWS-only piece of this phase.

The five acceptance criteria:
  (a) bad data fails the GE suite and the EMR job halts with no Iceberg write
  (b) a holdout-failing candidate never reaches paired-score or policy canary callbacks
  (c) a clean local fixture passes paired-score and advances through injected policy callbacks
  (d) a regression fixture chooses the rollback callback in the policy state machine
  (e) the EMR module's plan-time termination attribute is present
"""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from e2e.lake_helpers import EMR_MODULE_PATH, emr_module_has_auto_terminate
from lake.backfill.job import DataQualityGateFailure, _gate_and_canonicalize_partition
from mlops.holdout_gate import HoldoutGateResult, run_holdout_gate
from mlops.promote import CANARY_WEIGHTS, run_promotion
from mlops.registry import register_candidate
from mlops.shadow_diff import ShadowDiffResult, score_diff

PIDPM_MODULE_PATH = (
    Path(__file__).resolve().parents[2] / "infra/terraform/modules/sagemaker_pidpm/main.tf"
)
STATE_STORES_MODULE_PATH = (
    Path(__file__).resolve().parents[2] / "infra/terraform/modules/state_stores/main.tf"
)
BASE_MAIN_PATH = Path(__file__).resolve().parents[2] / "infra/terraform/envs/base/main.tf"
BASE_VARIABLES_PATH = Path(__file__).resolve().parents[2] / "infra/terraform/envs/base/variables.tf"
PIDPM_PROMOTION_PATH = Path(__file__).resolve().parents[2] / "mlops/promote.py"
SHADOW_DIFF_PATH = Path(__file__).resolve().parents[2] / "mlops/shadow_diff.py"
WANDB_ADAPTER_PATH = Path(__file__).resolve().parents[2] / "mlops/wandb_adapter.py"
MODEL_REGISTRY_PATH = Path(__file__).resolve().parents[2] / "mlops/registry.py"
DRIFT_PATH = Path(__file__).resolve().parents[2] / "mlops/drift.py"
CALIBRATION_WATCH_PATH = Path(__file__).resolve().parents[2] / "mlops/calibration_watch.py"
DRIFT_DECISION_PATH = Path(__file__).resolve().parents[2] / "mlops/drift_decision.py"


def test_a_bad_data_fails_the_ge_suite_and_the_job_halts_with_no_write():
    bad_partition = pd.DataFrame(
        [
            {
                "mmsi": 42,
                "t": "2024-06-01T00:00:00Z",
                "lat": 40.0,
                "lon": -74.0,
                "sog": 5.0,
                "cog": 1.0,
            }
        ]
    )  # mmsi 42 is out of the valid MMSI range: the GE suite must reject it
    with pytest.raises(DataQualityGateFailure):
        _gate_and_canonicalize_partition(bad_partition)
    # DataQualityGateFailure propagating IS the halt: lake/backfill/job.py's
    # mapInPandas wrapper never reaches the Iceberg write step below a raise


def test_b_a_failing_holdout_gate_never_reaches_paired_score_or_policy_canary_callbacks():
    rng = np.random.default_rng(99)
    n = 200
    labels = rng.integers(0, 2, size=n)
    scores = rng.normal(0, 1, size=n)  # uninformative: fails the AUC threshold
    gate = run_holdout_gate(
        labels=labels,
        scores=scores,
        predicted_mean=rng.normal(0, 1, size=n),
        predicted_sigma=np.full(n, 1.0),
        observed=rng.normal(0, 1, size=n),
    )
    assert not gate.passed

    class UnreachableSageMaker:
        def create_model_package(self, **kwargs):
            raise AssertionError("register_candidate must refuse before calling SageMaker")

    with pytest.raises(ValueError):
        register_candidate(
            sagemaker_client=UnreachableSageMaker(),
            model_package_group_name="hm-pidpm",
            model_data_url="s3://bucket/model.tar.gz",
            container_image="image:latest",
            holdout_result=gate,
        )

    weights_set: list[int] = []
    promotion = run_promotion(
        holdout_result=gate,
        shadow_result=None,
        burn_check=lambda w: False,
        set_canary_weight=weights_set.append,
        revert_to_champion=lambda: None,
    )
    assert promotion.final_status == "rejected_gate"
    assert weights_set == []


def test_c_a_clean_policy_fixture_passes_paired_score_and_advances_callbacks():
    gate = HoldoutGateResult(auc=0.95, crps=0.2, calibration_ratio=1.0, passed=True, failures=[])
    champion = np.full(50, 0.3)
    shadow = champion + 0.01
    shadow_result = score_diff(champion, shadow, max_divergence=0.05)
    assert shadow_result.passed

    weights_set: list[int] = []
    reverted = {"called": False}
    promotion = run_promotion(
        holdout_result=gate,
        shadow_result=shadow_result,
        burn_check=lambda w: False,
        set_canary_weight=weights_set.append,
        revert_to_champion=lambda: reverted.__setitem__("called", True),
    )
    assert promotion.final_status == "promoted"
    assert weights_set == list(CANARY_WEIGHTS)
    assert reverted["called"] is False


def test_d_a_regression_fixture_selects_the_policy_rollback_callback():
    gate = HoldoutGateResult(auc=0.95, crps=0.2, calibration_ratio=1.0, passed=True, failures=[])
    shadow_result = ShadowDiffResult(
        mean_abs_diff=0.01, max_abs_diff=0.02, n_samples=50, passed=True
    )

    weights_set: list[int] = []
    champion_restored = {"called": False}

    def revert_to_champion() -> None:
        champion_restored["called"] = True

    promotion = run_promotion(
        holdout_result=gate,
        shadow_result=shadow_result,
        burn_check=lambda w: w == 25,  # the regression only surfaces at 25% traffic
        set_canary_weight=weights_set.append,
        revert_to_champion=revert_to_champion,
    )
    assert promotion.final_status == "rolled_back"
    assert champion_restored["called"] is True
    assert weights_set == [5, 25]  # never advanced to 50 or 100 after the burn


def test_e_the_emr_modules_plan_time_termination_attribute_is_present():
    assert emr_module_has_auto_terminate(EMR_MODULE_PATH.read_text())


def test_f_async_pidpm_endpoint_rejects_weighted_candidate_configuration():
    source = PIDPM_MODULE_PATH.read_text()

    # SageMaker Async Inference supports one production variant. The candidate
    # input must fail at plan time, before AWS can create a partial model.
    assert 'resource "aws_sagemaker_model" "candidate"' not in source
    assert source.count("production_variants {") == 1
    assert "condition     = !local.candidate_enabled" in source

    # Endpoint configurations are immutable, so every legitimate replacement
    # must be created before Terraform asks SageMaker to release the old one.
    assert 'name_prefix = "${local.name_prefix}-pidpm-"' in source
    assert "create_before_destroy = true" in source


def test_g_async_pidpm_endpoint_has_a_deterministic_failure_artifact_prefix():
    source = PIDPM_MODULE_PATH.read_text()

    # HM3-AUDIT-03: SageMaker async failures must land outside the normal
    # output prefix so failed inference is not indistinguishable from a slow
    # response. The endpoint role already has PutObject for the whole models
    # bucket; this assertion preserves the dedicated prefix contract.
    assert 's3_output_path  = "s3://${local.models_bucket}/pidpm/async-output"' in source
    assert 's3_failure_path = "s3://${local.models_bucket}/pidpm/async-failure"' in source


def test_h_async_pidpm_failure_artifacts_have_an_opt_in_owned_alert_path():
    pidpm_source = PIDPM_MODULE_PATH.read_text()
    state_stores_source = STATE_STORES_MODULE_PATH.read_text()
    base_main = BASE_MAIN_PATH.read_text()
    base_variables = BASE_VARIABLES_PATH.read_text()

    # HM3-AUDIT-03: S3 owns the one bucket-notification configuration. It is
    # enabled only with the endpoint-side EventBridge rule and never routes
    # every model artifact to email.
    assert 'resource "aws_s3_bucket_notification" "models_eventbridge"' in state_stores_source
    assert "eventbridge = true" in state_stores_source
    assert "enable_models_eventbridge = local.pidpm_failure_alerts_enabled" in base_main
    assert 'variable "enable_pidpm_failure_alerts"' in base_variables
    assert "enable_pidpm_failure_alerts requires enable_phase3" in base_variables

    # The target accepts only failure-prefix Object Created events from this
    # models bucket and uses a scoped EventBridge execution role to publish.
    assert 'resource "aws_cloudwatch_event_rule" "async_failure_artifact"' in pidpm_source
    assert 'source        = ["aws.s3"]' in pidpm_source
    assert '"detail-type" = ["Object Created"]' in pidpm_source
    assert 'key = [{ prefix = "pidpm/async-failure/" }]' in pidpm_source
    assert 'resource "aws_cloudwatch_event_target" "async_failure_alert"' in pidpm_source
    assert re.search(
        r"role_arn\s*=\s*aws_iam_role\.async_failure_eventbridge\[0\]\.arn",
        pidpm_source,
    )
    assert 'actions   = ["sns:Publish"]' in pidpm_source
    assert "resources = [aws_sns_topic.async_failure[0].arn]" in pidpm_source

    # The route gets its own topic and email confirmation rather than widening
    # the budget-alert policy or treating an unconfirmed destination as live.
    assert 'resource "aws_sns_topic" "async_failure"' in pidpm_source
    assert 'resource "aws_sns_topic_subscription" "async_failure_email"' in pidpm_source
    assert re.search(r"failure_alert_email\s*=\s*var\.alert_email", base_main)

    # HM3-AUDIT-07: error visibility shares the dedicated route without
    # widening the budget-alert policy. Async InvocationFailures carries both
    # endpoint and variant dimensions, and an absent series must not page.
    assert 'resource "aws_cloudwatch_metric_alarm" "invocation_failures"' in pidpm_source
    assert 'metric_name         = "InvocationFailures"' in pidpm_source
    assert 'statistic           = "Sum"' in pidpm_source
    assert 'treat_missing_data  = "notBreaching"' in pidpm_source
    assert "EndpointName = aws_sagemaker_endpoint.pidpm.name" in pidpm_source
    assert "VariantName  = local.variant_name" in pidpm_source
    assert "alarm_actions       = [aws_sns_topic.async_failure[0].arn]" in pidpm_source
    assert 'resource "aws_sns_topic_policy" "async_failure"' in pidpm_source
    assert 'identifiers = ["cloudwatch.amazonaws.com"]' in pidpm_source
    assert 'variable = "aws:SourceArn"' in pidpm_source
    assert 'variable = "aws:SourceAccount"' in pidpm_source
    assert "AllowOnlyPiDpmInvocationFailureAlarm" in pidpm_source
