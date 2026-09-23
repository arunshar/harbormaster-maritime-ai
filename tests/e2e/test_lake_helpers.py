from __future__ import annotations

from e2e.lake_helpers import EMR_MODULE_PATH, emr_module_has_auto_terminate


def test_emr_module_has_auto_terminate_true_on_a_real_enabled_block():
    text = """
    resource "aws_emrserverless_application" "backfill" {
      auto_stop_configuration {
        enabled              = true
        idle_timeout_minutes = 15
      }
    }
    """
    assert emr_module_has_auto_terminate(text) is True


def test_emr_module_has_auto_terminate_false_when_block_absent():
    assert emr_module_has_auto_terminate('resource "aws_emrserverless_application" "x" {}') is False


def test_emr_module_has_auto_terminate_false_when_disabled():
    text = """
    auto_stop_configuration {
      enabled = false
    }
    """
    assert emr_module_has_auto_terminate(text) is False


def test_the_real_committed_emr_module_has_auto_terminate():
    assert emr_module_has_auto_terminate(EMR_MODULE_PATH.read_text()) is True
