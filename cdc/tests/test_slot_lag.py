"""Gates C7/C8: slot-lag monitor core + the Lambda's metric shaping."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from cdc.monitor.slot_lag import (
    DEFAULT_LAG_ALARM_BYTES,
    SLOT_LAG_SQL,
    SlotLag,
    evaluate_lag_alert,
    rows_to_slot_lags,
)

HANDLER_PATH = (
    Path(__file__).parent.parent.parent / "infra" / "lambda" / "cdc_slot_lag" / "handler.py"
)


def _load_handler():
    spec = importlib.util.spec_from_file_location("cdc_slot_lag_handler", HANDLER_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_sql_anchors_on_confirmed_flush_with_restart_fallback():
    assert "confirmed_flush_lsn" in SLOT_LAG_SQL
    assert "restart_lsn" in SLOT_LAG_SQL
    assert "pg_replication_slots" in SLOT_LAG_SQL


def test_rows_to_slot_lags_types_and_null_lag():
    slots = rows_to_slot_lags(
        [
            ("harbormaster_cdc", False, 1024),
            ("other", True, None),  # slot created, consumer never connected
        ]
    )
    assert slots[0] == SlotLag("harbormaster_cdc", False, 1024)
    assert slots[1].lag_bytes == 0


def test_evaluate_lag_alert_threshold_edges():
    slots = [
        SlotLag("a", False, 100),
        SlotLag("b", False, 200),
        SlotLag("c", True, 300),
    ]
    breaching = evaluate_lag_alert(slots, threshold_bytes=200)
    assert [s.slot_name for s in breaching] == ["b", "c"]  # >= threshold, active or not
    assert evaluate_lag_alert(slots, threshold_bytes=10_000) == []


def test_evaluate_lag_alert_rejects_nonpositive_threshold():
    with pytest.raises(ValueError):
        evaluate_lag_alert([], threshold_bytes=0)


def test_default_threshold_matches_the_terraform_default():
    # modules/cdc_monitoring var slot_lag_alarm_bytes default = 209715200
    assert DEFAULT_LAG_ALARM_BYTES == 209_715_200


def test_lambda_metric_data_shapes_both_metrics_per_slot():
    handler = _load_handler()
    data = handler.metric_data(
        [SlotLag("harbormaster_cdc", False, 4096), SlotLag("other", True, 0)]
    )
    assert len(data) == 4
    lag = data[0]
    assert lag["MetricName"] == "ReplicationSlotLagBytes"
    assert lag["Dimensions"] == [{"Name": "SlotName", "Value": "harbormaster_cdc"}]
    assert lag["Value"] == 4096.0 and lag["Unit"] == "Bytes"
    active = data[1]
    assert active["MetricName"] == "SlotActive" and active["Value"] == 0.0
    assert handler.METRIC_NAMESPACE == "Harbormaster/CDC"
