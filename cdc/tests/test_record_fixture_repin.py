"""The recorder's offline re-pin step must reproduce the committed CDC pins.

scripts/cdc_record_fixture.py derives the envelope SHA256, the envelope and
apply censuses, and the final-state SHA256 from the committed envelope file.
These tests run that step without a live stack and compare the result with
cdc/fixtures/expectations.json. They also check that the fixture keeps its
fictional identifiers.
"""

from __future__ import annotations

import json

from cdc.fixtures.loader import EXPECTATIONS_PATH, load_envelope_messages, load_expectations
from scripts import cdc_record_fixture as recorder

FICTIONAL_MID = "200"  # not allocated to any country in the ITU MID table


def test_repin_reproduces_the_committed_expectations():
    assert recorder.repin_expectations() == load_expectations()


def test_repin_writes_the_committed_bytes(tmp_path):
    out = tmp_path / "expectations.json"
    returned = recorder.repin_expectations(out)
    assert out.read_bytes() == EXPECTATIONS_PATH.read_bytes()
    assert json.loads(out.read_text()) == returned


def test_repin_only_cli_needs_no_live_stack(tmp_path, monkeypatch, capsys):
    out = tmp_path / "expectations.json"
    monkeypatch.setattr(recorder, "EXPECTATIONS_PATH", out)

    def _no_connect_call() -> bool:
        raise AssertionError("--repin-only must not contact Kafka Connect")

    monkeypatch.setattr(recorder, "connector_exists", _no_connect_call)
    assert recorder.main(["--repin-only"]) == 0
    assert json.loads(out.read_text()) == load_expectations()
    assert "no live capture" in capsys.readouterr().out


def test_unknown_arguments_are_rejected(monkeypatch, capsys):
    def _no_connect_call() -> bool:
        raise AssertionError("a usage error must not contact Kafka Connect")

    monkeypatch.setattr(recorder, "connector_exists", _no_connect_call)
    assert recorder.main(["--repin"]) == 2
    assert "usage" in capsys.readouterr().out


def _mmsis(node: object) -> set[int]:
    found: set[int] = set()
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "mmsi" and isinstance(value, int):
                found.add(value)
            else:
                found |= _mmsis(value)
    elif isinstance(node, list):
        for item in node:
            found |= _mmsis(item)
    return found


def test_fixture_vessels_and_sanctions_reference_are_fictional():
    mmsis: set[int] = set()
    references: set[str] = set()
    for _topic, key, value in load_envelope_messages():
        mmsis |= _mmsis(json.loads(key))
        if value is not None:
            payload = json.loads(value)
            mmsis |= _mmsis(payload)
            after = payload.get("after") or {}
            if "reference" in after:
                references.add(after["reference"])
    assert mmsis == {200000001, 200000003}
    assert all(str(m).startswith(FICTIONAL_MID) and len(str(m)) == 9 for m in mmsis)
    assert references == {"SDN-EXAMPLE-0001"}
    assert recorder.SNAPSHOT_MMSI in mmsis and recorder.LIVE_MMSI in mmsis
