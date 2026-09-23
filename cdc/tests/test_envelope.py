"""Gate C3: Debezium envelope parsing over the committed fixture + error paths."""

from __future__ import annotations

import json

import pytest

from cdc.consumer.envelope import (
    ChangeEvent,
    EnvelopeError,
    Skip,
    Tombstone,
    parse_envelope,
)
from cdc.fixtures.loader import envelopes_sha256, load_envelope_messages, load_expectations

TENANT = "00000000-0000-0000-0000-000000000000"


def _parse_all() -> list:
    return [parse_envelope(t, k, v) for t, k, v in load_envelope_messages()]


def test_fixture_sha256_is_pinned():
    exp = load_expectations()
    assert envelopes_sha256() == exp["debezium_envelopes_sha256"], (
        "the envelope fixture changed; if intentional (e.g. the gate-C6 re-record), "
        "update cdc/fixtures/expectations.json in the same commit"
    )


def test_fixture_message_census():
    parsed = _parse_all()
    exp = load_expectations()["envelope_census"]
    assert sum(isinstance(p, ChangeEvent) for p in parsed) == exp["change_events"]
    assert sum(isinstance(p, Tombstone) for p in parsed) == exp["tombstones"]
    assert sum(isinstance(p, Skip) for p in parsed) == exp["skips"]


def test_snapshot_reads_carry_the_snapshot_lsn():
    # both op=r rows share the snapshot consistent-point LSN (recorded fixture)
    parsed = _parse_all()
    reads = [p for p in parsed if isinstance(p, ChangeEvent) and p.is_snapshot]
    first = parsed[0]
    assert isinstance(first, ChangeEvent)
    assert first.op == "r" and first.is_snapshot
    assert first.table == "vessels" and first.lsn > 0
    assert first.pk == {"tenant_id": TENANT, "mmsi": 200000001}
    assert len({r.lsn for r in reads}) == 1


def test_fixture_update_carries_both_images():
    update = [p for p in _parse_all() if isinstance(p, ChangeEvent) and p.op == "u"][0]
    assert update.pk == {"tenant_id": TENANT, "mmsi": 200000003}
    assert update.after is not None and update.after["severity"] == 0.95
    assert update.before is not None and update.before["severity"] == 0.9


def test_schema_wrapped_converter_output_unwraps():
    # the live plane runs schemas.enable=false, so the recorded fixture has no
    # wrapped line; wrap the recorded update here and assert the parse is
    # identical (the unwrap path stays covered for schemas.enable=true planes)
    topic, key, value = [
        (t, k, v)
        for t, k, v in load_envelope_messages()
        if v is not None and json.loads(v).get("op") == "u"
    ][0]
    plain = parse_envelope(topic, key, value)
    wrapped = parse_envelope(
        topic,
        json.dumps({"schema": {"type": "struct"}, "payload": json.loads(key)}),
        json.dumps({"schema": {"type": "struct"}, "payload": json.loads(value)}),
    )
    assert wrapped == plain


def test_delete_has_before_image_and_no_after():
    parsed = _parse_all()
    delete = [p for p in parsed if isinstance(p, ChangeEvent) and p.is_delete][0]
    update = [p for p in parsed if isinstance(p, ChangeEvent) and p.op == "u"][0]
    assert delete.table == "watchlist" and delete.pk == {
        "tenant_id": TENANT,
        "mmsi": 200000001,
    }
    assert delete.after is None
    # REPLICA IDENTITY FULL: the delete carries the full before image
    assert delete.before is not None and delete.before["reason"] == "legacy flag"
    assert delete.lsn > update.lsn


def test_tombstone_carries_the_pk_and_nothing_else():
    ts = [p for p in _parse_all() if isinstance(p, Tombstone)][0]
    assert ts.pk == {"tenant_id": TENANT, "mmsi": 200000001}


def test_heartbeat_and_schema_change_are_typed_skips():
    skips = [p for p in _parse_all() if isinstance(p, Skip)]
    assert {s.reason for s in skips} == {"heartbeat", "schema_change"}


def test_redelivered_duplicate_parses_identically():
    events = [p for p in _parse_all() if isinstance(p, ChangeEvent)]
    dupes = [
        e
        for e in events
        if e.op == "c"
        and e.table == "watchlist"
        and e.pk == {"tenant_id": TENANT, "mmsi": 200000003}
    ]
    assert len(dupes) == 2 and dupes[0] == dupes[1]


# ------------------------------------------------------------------- errors


def _valid_value(**overrides) -> str:
    v = {
        "before": None,
        "after": {"tenant_id": TENANT, "mmsi": 1, "reason": "x"},
        "source": {"table": "watchlist", "lsn": 10},
        "op": "c",
        "ts_ms": 1,
    }
    v.update(overrides)
    return json.dumps(v)


def test_non_json_value_raises():
    with pytest.raises(EnvelopeError, match="not JSON"):
        parse_envelope("hm.public.watchlist", '{"mmsi": 1}', "{not json")


def test_missing_op_without_ddl_raises():
    with pytest.raises(EnvelopeError, match="no op"):
        parse_envelope("hm.public.watchlist", '{"mmsi": 1}', json.dumps({"after": {}}))


def test_unknown_op_raises():
    with pytest.raises(EnvelopeError, match="unknown op"):
        parse_envelope("hm.public.watchlist", '{"mmsi": 1}', _valid_value(op="z"))


def test_missing_lsn_raises():
    with pytest.raises(EnvelopeError, match="no source.lsn"):
        parse_envelope(
            "hm.public.watchlist", '{"mmsi": 1}', _valid_value(source={"table": "watchlist"})
        )


def test_missing_key_on_data_event_raises():
    with pytest.raises(EnvelopeError, match="no key"):
        parse_envelope("hm.public.watchlist", None, _valid_value())


def test_legacy_registry_event_without_tenant_normalizes_to_the_sentinel():
    event = parse_envelope(
        "hm.public.watchlist",
        '{"mmsi": 1}',
        _valid_value(after={"mmsi": 1, "reason": "x"}),
    )
    assert isinstance(event, ChangeEvent)
    assert event.pk == {"tenant_id": TENANT, "mmsi": 1}
    assert event.delivered_pk == {"mmsi": 1}
    assert event.after == {"tenant_id": TENANT, "mmsi": 1, "reason": "x"}
    assert event.delivered_after == {"mmsi": 1, "reason": "x"}


def test_legacy_and_composite_keys_converge_to_one_identity():
    legacy = parse_envelope("hm.public.watchlist", '{"mmsi": 1}', _valid_value())
    composite = parse_envelope(
        "hm.public.watchlist",
        json.dumps({"tenant_id": TENANT, "mmsi": 1}),
        _valid_value(after={"tenant_id": TENANT, "mmsi": 1, "reason": "x"}),
    )
    assert isinstance(legacy, ChangeEvent) and isinstance(composite, ChangeEvent)
    assert legacy.pk == composite.pk
    assert legacy.after == composite.after
    assert legacy.delivered_pk != composite.delivered_pk


def test_invalid_tenant_id_fails_closed():
    with pytest.raises(EnvelopeError, match="invalid tenant_id"):
        parse_envelope(
            "hm.public.watchlist",
            json.dumps({"tenant_id": "not-a-uuid", "mmsi": 1}),
            _valid_value(),
        )


@pytest.mark.parametrize("tenant_id", ["", None])
def test_explicit_blank_or_null_tenant_id_fails_closed(tenant_id):
    with pytest.raises(EnvelopeError, match="invalid tenant_id"):
        parse_envelope(
            "hm.public.watchlist",
            json.dumps({"tenant_id": tenant_id, "mmsi": 1}),
            _valid_value(after={"mmsi": 1, "reason": "x"}),
        )


def test_registry_event_with_mismatched_tenant_fails_closed():
    key = json.dumps({"tenant_id": TENANT, "mmsi": 1})
    other = "11111111-1111-1111-1111-111111111111"
    with pytest.raises(EnvelopeError, match="mismatched tenant_id"):
        parse_envelope(
            "hm.public.watchlist",
            key,
            _valid_value(after={"tenant_id": other, "mmsi": 1, "reason": "x"}),
        )


def test_semantically_equal_uuid_spellings_normalize_before_comparison():
    upper = TENANT.upper()
    event = parse_envelope(
        "hm.public.watchlist",
        json.dumps({"tenant_id": upper, "mmsi": 1}),
        _valid_value(),
    )
    assert isinstance(event, ChangeEvent) and event.pk["tenant_id"] == TENANT
