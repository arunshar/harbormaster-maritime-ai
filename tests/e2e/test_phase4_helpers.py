from __future__ import annotations

from e2e.phase4_helpers import ENV_MAIN_TF_PATH, drift_watch_module_is_gated_on_enable_phase4


def test_gated_true_on_a_real_matching_module_block():
    text = """
    module "drift_watch" {
      count  = var.enable_phase4 ? 1 : 0
      source = "../../modules/drift_watch"
    }
    """
    assert drift_watch_module_is_gated_on_enable_phase4(text) is True


def test_gated_false_when_module_absent():
    assert drift_watch_module_is_gated_on_enable_phase4('module "other" { count = 1 }') is False


def test_gated_false_when_gated_on_a_different_variable():
    text = """
    module "drift_watch" {
      count  = var.enable_phase3 ? 1 : 0
    }
    """
    assert drift_watch_module_is_gated_on_enable_phase4(text) is False


def test_gated_false_when_hardcoded_on():
    text = """
    module "drift_watch" {
      count = 1
    }
    """
    assert drift_watch_module_is_gated_on_enable_phase4(text) is False


def test_the_real_committed_env_main_tf_gates_drift_watch_on_enable_phase4():
    assert drift_watch_module_is_gated_on_enable_phase4(ENV_MAIN_TF_PATH.read_text()) is True
