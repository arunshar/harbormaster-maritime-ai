from __future__ import annotations

from e2e.kms_helpers import (
    ENV_MAIN_TF_PATH,
    MODULES_PATH,
    consumer_kms_key_arn_defaults,
    kms_key_arn_local_collapses_to_empty,
    kms_module_is_gated_on_enable_cmk,
    modules_wired_with_kms_key_arn,
)

# The module set that owns encrypted resources: the S3/DynamoDB stores, RDS
# storage, and every CloudWatch-log-group owner. envs/base must wire each one,
# and each must default its kms_key_arn to "" (the zero-diff collapse).
EXPECTED_CONSUMERS = {
    "state_stores",
    "rds",
    "apigw",
    "drift_watch",
    "ecs_cdc_consumer",
    "ecs_connect",
    "ecs_ingestor",
    "ecs_serving",
    "emr_backfill",
    "kda_flink",
    "redis_fargate",
}


def test_gated_true_on_a_real_matching_module_block():
    text = """
    module "kms" {
      count  = var.enable_cmk ? 1 : 0
      source = "../../modules/kms"
    }
    """
    assert kms_module_is_gated_on_enable_cmk(text) is True


def test_gated_false_when_module_absent():
    assert kms_module_is_gated_on_enable_cmk('module "other" { count = 1 }') is False


def test_gated_false_when_gated_on_a_different_variable():
    text = """
    module "kms" {
      count = var.enable_phase1 ? 1 : 0
    }
    """
    assert kms_module_is_gated_on_enable_cmk(text) is False


def test_gated_false_when_hardcoded_on():
    text = """
    module "kms" {
      count = 1
    }
    """
    assert kms_module_is_gated_on_enable_cmk(text) is False


def test_local_collapse_true_on_the_exact_ternary():
    text = 'kms_key_arn = var.enable_cmk ? module.kms[0].key_arn : ""'
    assert kms_key_arn_local_collapses_to_empty(text) is True


def test_local_collapse_false_when_fallback_is_not_empty():
    text = 'kms_key_arn = var.enable_cmk ? module.kms[0].key_arn : "alias/aws/s3"'
    assert kms_key_arn_local_collapses_to_empty(text) is False


def test_wired_modules_found_and_attributed_to_the_enclosing_call():
    text = """
    module "first" {
      kms_key_arn = local.kms_key_arn
    }
    module "second" {
      other = 1
    }
    module "third" {
      kms_key_arn = local.kms_key_arn
    }
    """
    assert modules_wired_with_kms_key_arn(text) == {"first", "third"}


def test_wired_modules_empty_when_no_module_header_precedes():
    assert modules_wired_with_kms_key_arn("kms_key_arn = local.kms_key_arn") == set()


def test_consumer_defaults_parsed_from_a_synthetic_tree(tmp_path):
    good = tmp_path / "good"
    good.mkdir()
    (good / "variables.tf").write_text(
        'variable "kms_key_arn" {\n  type    = string\n  default = ""\n}\n'
    )
    bad = tmp_path / "bad"
    bad.mkdir()
    (bad / "main.tf").write_text('variable "kms_key_arn" {\n  type = string\n}\n')
    unrelated = tmp_path / "unrelated"
    unrelated.mkdir()
    (unrelated / "main.tf").write_text('variable "tags" {\n  default = {}\n}\n')

    assert consumer_kms_key_arn_defaults(tmp_path) == {"good": "", "bad": None}


def test_the_real_committed_env_main_tf_gates_kms_on_enable_cmk():
    assert kms_module_is_gated_on_enable_cmk(ENV_MAIN_TF_PATH.read_text()) is True


def test_the_real_committed_env_main_tf_local_collapses_to_empty():
    assert kms_key_arn_local_collapses_to_empty(ENV_MAIN_TF_PATH.read_text()) is True


def test_the_real_committed_env_main_tf_wires_every_expected_consumer():
    assert modules_wired_with_kms_key_arn(ENV_MAIN_TF_PATH.read_text()) == EXPECTED_CONSUMERS


def test_every_real_consumer_module_defaults_kms_key_arn_to_empty():
    defaults = consumer_kms_key_arn_defaults(MODULES_PATH)
    assert set(defaults) == EXPECTED_CONSUMERS
    assert all(value == "" for value in defaults.values()), defaults
