"""Production-wiring regressions for the Managed Flink job."""

from __future__ import annotations

import ast
import inspect
import json
from pathlib import Path

import pytest

from flink import transforms, window_logic

JOB_PATH = Path(__file__).parents[1] / "job.py"
TRANSFORM_MODULES = (
    pytest.param(transforms, id="source"),
    pytest.param(window_logic, id="packaged-runtime"),
)


@pytest.mark.parametrize("module", TRANSFORM_MODULES)
@pytest.mark.parametrize("raw_mmsi", (200000001, "200000001"))
def test_mmsi_partition_key_preserves_valid_long_key(module, raw_mmsi):
    raw = json.dumps(
        {
            "mmsi": raw_mmsi,
            "lat": 40.4,
            "lon": -74.0,
            "t": "2024-06-01T00:00:00Z",
            "sog": 9.7,
            "cog": 180.0,
            "heading": 179.0,
        }
    )

    key = module.mmsi_partition_key(raw)

    assert key == 200000001
    assert isinstance(key, int)


@pytest.mark.parametrize("module", TRANSFORM_MODULES)
@pytest.mark.parametrize(
    "raw",
    [
        "{not json",
        "{}",
        json.dumps(
            {
                "mmsi": "not-a-number",
                "lat": 40.4,
                "lon": -74.0,
                "t": "2024-06-01T00:00:00Z",
            }
        ),
        json.dumps({"mmsi": 200000001}),
        json.dumps(
            {
                "mmsi": -1,
                "lat": 40.4,
                "lon": -74.0,
                "t": "2024-06-01T00:00:00Z",
            }
        ),
        json.dumps(
            {
                "mmsi": 1_000_000_000,
                "lat": 40.4,
                "lon": -74.0,
                "t": "2024-06-01T00:00:00Z",
            }
        ),
        json.dumps(
            {
                "mmsi": True,
                "lat": 40.4,
                "lon": -74.0,
                "t": "2024-06-01T00:00:00Z",
            }
        ),
        json.dumps(
            {
                "mmsi": 1.5,
                "lat": 40.4,
                "lon": -74.0,
                "t": "2024-06-01T00:00:00Z",
            }
        ),
        '{"mmsi":1e400,"lat":40.4,"lon":-74.0,"t":"2024-06-01T00:00:00Z"}',
        json.dumps(
            {
                "mmsi": "+1",
                "lat": 40.4,
                "lon": -74.0,
                "t": "2024-06-01T00:00:00Z",
            }
        ),
    ],
)
def test_mmsi_partition_key_routes_malformed_records_without_raising(module, raw):
    assert module.mmsi_partition_key(raw) == module.INVALID_AIS_KEY


@pytest.mark.parametrize("module", TRANSFORM_MODULES)
def test_deeply_nested_json_is_normalized_before_keyed_state(module):
    raw = "[" * 10_000 + "0" + "]" * 10_000

    with pytest.raises(ValueError, match="malformed ais record"):
        module.parse_ais_json(raw)
    assert module.mmsi_partition_key(raw) == module.INVALID_AIS_KEY


@pytest.mark.parametrize("module", TRANSFORM_MODULES)
def test_score_request_copy_includes_fix_and_history(module):
    fix = module.Fix(
        lat=40.4,
        lon=-74.0,
        t=module.datetime.fromisoformat("2024-06-01T00:01:00+00:00"),
        sog=9.7,
    )
    previous = module.Fix(
        lat=40.3,
        lon=-74.1,
        t=module.datetime.fromisoformat("2024-06-01T00:00:00+00:00"),
    )

    request = module.score_request(200000001, fix, [previous])

    assert request["mmsi"] == 200000001
    assert request["fix"]["t"] == "2024-06-01T00:01:00Z"
    assert request["history"][0]["t"] == "2024-06-01T00:00:00Z"


def test_runtime_mmsi_helpers_match_source_of_truth_copy():
    assert inspect.getsource(window_logic.parse_ais_json) == inspect.getsource(
        transforms.parse_ais_json
    )
    assert inspect.getsource(window_logic.mmsi_partition_key) == inspect.getsource(
        transforms.mmsi_partition_key
    )


def test_production_key_by_uses_non_throwing_long_selector():
    tree = ast.parse(JOB_PATH.read_text())
    key_by_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "key_by"
    ]

    assert len(key_by_calls) == 1
    call = key_by_calls[0]
    assert isinstance(call.args[0], ast.Name)
    assert call.args[0].id == "mmsi_partition_key"
    key_type = next(keyword.value for keyword in call.keywords if keyword.arg == "key_type")
    assert ast.unparse(key_type) == "Types.LONG()"


def test_parse_failure_quarantines_before_keyed_state_access():
    tree = ast.parse(JOB_PATH.read_text())
    feature_process = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "FeatureProcess"
    )
    process_element = next(
        node
        for node in feature_process.body
        if isinstance(node, ast.FunctionDef) and node.name == "process_element"
    )
    parse_try = next(node for node in process_element.body if isinstance(node, ast.Try))
    value_error_handler = next(
        handler
        for handler in parse_try.handlers
        if isinstance(handler.type, ast.Name) and handler.type.id == "ValueError"
    )
    quarantine_call = next(
        node
        for node in ast.walk(value_error_handler)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "_quarantine"
    )
    history_value_call = next(
        node
        for node in ast.walk(process_element)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "value"
        and isinstance(node.func.value, ast.Attribute)
        and node.func.value.attr == "_history"
    )

    assert any(isinstance(node, ast.Return) for node in ast.walk(value_error_handler))
    assert quarantine_call.lineno < history_value_call.lineno
