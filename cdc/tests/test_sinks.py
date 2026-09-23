"""Gate C5: the real sinks. Item shapes golden, guard semantics in lockstep with
the reference MemorySink, key vocabulary drift-guarded against serving."""

from __future__ import annotations

import json
from typing import Any

import pytest

from cdc.consumer.applier import Applier
from cdc.consumer.envelope import ChangeEvent, parse_envelope
from cdc.fixtures.loader import load_envelope_messages
from cdc.sinks import dynamo as dyn
from cdc.sinks.base import MemoryAudit, MemorySink
from cdc.sinks.dynamo import (
    CONDITION_EXPRESSION,
    OnlineStoreSink,
    item_for_soft_delete,
    item_for_upsert,
    key_for,
)
from cdc.sinks.iceberg_audit import CdcAuditSink, audit_row
from cdc.sinks.redis_cache import (
    REDIS_KEY_PREFIX,
    RedisInvalidationSink,
    redis_key_for_event,
)

MMSI = 200000003
TENANT_A = "00000000-0000-0000-0000-000000000000"
TENANT_B = "11111111-1111-1111-1111-111111111111"


def _pk(mmsi: int = MMSI, tenant_id: str = TENANT_A) -> dict[str, Any]:
    return {"tenant_id": tenant_id, "mmsi": mmsi}


def _sanction_pk(tenant_id: str = TENANT_A) -> dict[str, Any]:
    return {"tenant_id": tenant_id, "id": f"{MMSI}:ofac"}


def _messages():
    return [parse_envelope(t, k, v) for t, k, v in load_envelope_messages()]


def _event(**overrides: Any) -> ChangeEvent:
    base: dict[str, Any] = dict(
        table="watchlist",
        pk=_pk(),
        op="c",
        lsn=2000,
        ts_ms=1,
        before=None,
        after={
            "tenant_id": TENANT_A,
            "mmsi": MMSI,
            "reason": "dark rendezvous",
            "severity": 0.9,
        },
    )
    base.update(overrides)
    return ChangeEvent(**base)


# --------------------------------------------------- drift guards vs serving


def test_key_vocabulary_matches_the_serving_lookup_exactly():
    from app import watchlist as serving_wl

    assert dyn.FEATURE_VESSEL_META == serving_wl.FEATURE_VESSEL_META
    assert dyn.FEATURE_WATCHLIST == serving_wl.FEATURE_WATCHLIST
    assert dyn.FEATURE_SANCTIONS_PREFIX == serving_wl.FEATURE_SANCTIONS_PREFIX
    assert f"{REDIS_KEY_PREFIX}{TENANT_A}:{MMSI}" == serving_wl.redis_key(MMSI, TENANT_A)
    assert key_for("watchlist", _pk())["entity_id"]["S"] == serving_wl.online_entity_id(
        MMSI, TENANT_A
    )


def test_sink_items_parse_back_through_the_serving_reader():
    from app.watchlist import parse_online_items

    items = [
        item_for_upsert("watchlist", _pk(), {"reason": "x", "severity": 0.8}, 10),
        item_for_upsert("vessels", _pk(), {"name": "HM DEMO VESSEL B"}, 11),
        item_for_upsert(
            "sanctions_flags", _sanction_pk(), {"regime": "ofac", "mmsi": MMSI}, 12
        ),
    ]
    status = parse_online_items(items)
    assert status.watchlisted and status.reason == "x" and status.severity == 0.8
    assert status.sanctions == ("ofac",)
    assert status.vessel["name"] == "HM DEMO VESSEL B"
    # and the delete marker reads as absent end to end
    status2 = parse_online_items([item_for_soft_delete("watchlist", _pk(), 13)])
    assert status2.watchlisted is False


# ------------------------------------------------------------- item shapes


def test_item_for_upsert_watchlist_golden():
    item = item_for_upsert(
        "watchlist",
        _pk(),
        {"tenant_id": TENANT_A, "mmsi": MMSI, "reason": "x", "severity": 0.9},
        2000,
    )
    assert item == {
        "entity_id": {"S": f"{TENANT_A}:{MMSI}"},
        "feature_name": {"S": "watchlist"},
        "last_applied_lsn": {"N": "2000"},
        "deleted": {"BOOL": False},
        "tenant_id": {"S": TENANT_A},
        "mmsi": {"N": str(MMSI)},
        "reason": {"S": "x"},
        "severity": {"N": "0.9"},
    }


def test_sanctions_key_mapping_splits_the_composite_id():
    key = key_for("sanctions_flags", _sanction_pk())
    assert key == {
        "entity_id": {"S": f"{TENANT_A}:{MMSI}"},
        "feature_name": {"S": "sanctions:ofac"},
    }
    with pytest.raises(ValueError, match="mmsi:regime"):
        key_for("sanctions_flags", {"tenant_id": TENANT_A, "id": "malformed"})


def test_missing_tenant_in_source_key_fails_closed():
    with pytest.raises(ValueError, match="no tenant_id"):
        key_for("watchlist", {"mmsi": MMSI})


@pytest.mark.parametrize("tenant_id", ["", None, "not-a-uuid"])
def test_invalid_tenant_in_source_key_fails_closed(tenant_id):
    with pytest.raises(ValueError, match="tenant_id"):
        key_for("watchlist", {"tenant_id": tenant_id, "mmsi": MMSI})


def test_unknown_table_fails_loud():
    with pytest.raises(ValueError, match="no online mapping"):
        key_for("shadow_table", {"id": 1})


def test_payload_cannot_shadow_key_or_guard_attributes():
    item = item_for_upsert(
        "watchlist", _pk(), {"deleted": True, "last_applied_lsn": 9999, "reason": "x"}, 5
    )
    assert item["deleted"] == {"BOOL": False}
    assert item["last_applied_lsn"] == {"N": "5"}


# --------------------------------------------- conditional guard (lockstep)


class ConditionalCheckFailedException(Exception):
    pass


class FakeDdbTable:
    """Enforces the exact conditional-put semantics DynamoDB would."""

    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict] = {}

    def put_item(self, TableName, Item, ConditionExpression, ExpressionAttributeValues):
        assert ConditionExpression == CONDITION_EXPRESSION
        key = (Item["entity_id"]["S"], Item["feature_name"]["S"])
        lsn = int(ExpressionAttributeValues[":lsn"]["N"])
        existing = self.items.get(key)
        if existing is not None and int(existing["last_applied_lsn"]["N"]) >= lsn:
            raise ConditionalCheckFailedException("The conditional request failed")
        self.items[key] = Item


def test_online_store_sink_matches_the_reference_memory_sink_in_lockstep():
    events = [m for m in _messages() if isinstance(m, ChangeEvent)]
    mem = MemorySink()
    fake = FakeDdbTable()
    ddb = OnlineStoreSink(client=fake, table_name="feast-online")

    for e in events:
        if e.is_delete:
            assert mem.soft_delete(e.table, e.pk, e.lsn) == ddb.soft_delete(e.table, e.pk, e.lsn)
        else:
            assert mem.upsert(e.table, e.pk, e.after or {}, e.lsn) == ddb.upsert(
                e.table, e.pk, e.after or {}, e.lsn
            )

    # same survivors, same guard positions, same tombstone markers
    assert len(fake.items) == len(mem.final_state())
    assert sum(v["deleted"] for v in mem.final_state().values()) == sum(
        item["deleted"]["BOOL"] for item in fake.items.values()
    )


def test_same_mmsi_in_two_tenants_has_independent_online_keys_and_lsn_guards():
    fake = FakeDdbTable()
    sink = OnlineStoreSink(client=fake, table_name="feast-online")
    assert sink.upsert("watchlist", _pk(tenant_id=TENANT_A), {"reason": "a"}, 20)
    assert sink.upsert("watchlist", _pk(tenant_id=TENANT_B), {"reason": "b"}, 10)
    assert sink.upsert("watchlist", _pk(tenant_id=TENANT_A), {"reason": "stale"}, 19) is False
    assert set(fake.items) == {
        (f"{TENANT_A}:{MMSI}", "watchlist"),
        (f"{TENANT_B}:{MMSI}", "watchlist"),
    }


def test_non_conditional_ddb_error_propagates():
    class BrokenDdb:
        def put_item(self, **kwargs):
            raise RuntimeError("throttled")

    sink = OnlineStoreSink(client=BrokenDdb(), table_name="t")
    with pytest.raises(RuntimeError, match="throttled"):
        sink.upsert("watchlist", _pk(), {}, 1)


def test_botocore_style_conditional_error_maps_to_guard_rejected():
    class BotocoreStyleError(Exception):
        def __init__(self):
            super().__init__("ConditionalCheckFailedException")
            self.response = {"Error": {"Code": "ConditionalCheckFailedException"}}

    class Ddb:
        def put_item(self, **kwargs):
            raise BotocoreStyleError()

    sink = OnlineStoreSink(client=Ddb(), table_name="t")
    assert sink.upsert("watchlist", _pk(), {}, 1) is False


# -------------------------------------------------------- redis invalidation


class FakeRedis:
    def __init__(self) -> None:
        self.deleted: list[str] = []

    def delete(self, key: str) -> None:
        self.deleted.append(key)


def test_redis_key_for_event_covers_all_three_tables():
    assert redis_key_for_event(_event()) == f"hm:online:{TENANT_A}:{MMSI}"
    assert redis_key_for_event(_event(table="vessels")) == f"hm:online:{TENANT_A}:{MMSI}"
    assert (
        redis_key_for_event(
            _event(table="sanctions_flags", pk=_sanction_pk(), after=None)
        )
        == f"hm:online:{TENANT_A}:{MMSI}"
    )
    assert redis_key_for_event(_event(pk=_pk(tenant_id=TENANT_B))) != redis_key_for_event(
        _event(pk=_pk(tenant_id=TENANT_A))
    )


def test_invalidation_fires_for_every_delivered_data_event():
    fake = FakeRedis()
    store = MemorySink()
    applier = Applier(
        store=store, effects=(RedisInvalidationSink(client=fake),), audit=MemoryAudit()
    )
    result = applier.apply_batch(_messages(), commit=lambda: None)
    # DEL is idempotent; guard-rejected redeliveries re-fire on purpose (the
    # prior attempt may have died between the store write and the DEL)
    assert len(fake.deleted) == result.events
    assert f"hm:online:{TENANT_A}:{MMSI}" in fake.deleted
    assert f"hm:online:{TENANT_A}:200000001" in fake.deleted


# ------------------------------------------------------------------- audit


def test_audit_row_golden():
    row = audit_row(_event(), applied=True, consumed_at_ms=123)
    assert row == {
        "event_table": "watchlist",
        "pk": f'{{"mmsi":200000003,"tenant_id":"{TENANT_A}"}}',
        "op": "c",
        "lsn": 2000,
        "ts_ms": 1,
        "before_json": None,
        "after_json": (
            '{"mmsi": 200000003, "reason": "dark rendezvous", "severity": 0.9, '
            f'"tenant_id": "{TENANT_A}"}}'
        ),
        "applied": True,
        "consumed_at_ms": 123,
    }


def test_audit_preserves_legacy_row_image_while_pk_uses_normalized_tenant():
    value = {
        "before": None,
        "after": {"mmsi": MMSI, "reason": "legacy"},
        "source": {"table": "watchlist", "lsn": 10},
        "op": "c",
        "ts_ms": 1,
    }
    event = parse_envelope("hm.public.watchlist", f'{{"mmsi":{MMSI}}}', json.dumps(value))
    assert isinstance(event, ChangeEvent)
    row = audit_row(event, applied=True, consumed_at_ms=2)
    assert row["pk"] == f'{{"mmsi":{MMSI}}}'
    assert json.loads(row["after_json"]) == {"mmsi": MMSI, "reason": "legacy"}


def test_audit_sink_buffers_flushes_and_requeues_on_writer_failure():
    written: list[list[dict]] = []
    sink = CdcAuditSink(writer=written.append, now_ms=lambda: 1)
    sink.append(_event(), True)
    sink.append(_event(lsn=3000, op="u"), False)
    assert written == []  # buffered until the batch-ack flush
    sink.flush()
    assert len(written) == 1 and [r["lsn"] for r in written[0]] == [2000, 3000]
    sink.flush()  # empty flush writes nothing
    assert len(written) == 1

    fails = {"n": 0}

    def flaky(rows):
        fails["n"] += 1
        raise RuntimeError("warehouse down")

    sink2 = CdcAuditSink(writer=flaky, now_ms=lambda: 1)
    sink2.append(_event(), True)
    with pytest.raises(RuntimeError):
        sink2.flush()
    # rows were requeued; a later flush (post-redelivery retry) still has them
    sink2._writer = written.append
    sink2.flush()
    assert [r["lsn"] for r in written[-1]] == [2000]


def test_fixture_through_real_audit_sink_counts_transport_truth():
    written: list[list[dict]] = []
    audit = CdcAuditSink(writer=written.append, now_ms=lambda: 7)
    applier = Applier(store=MemorySink(), audit=audit)
    result = applier.apply_batch(_messages(), commit=lambda: None)
    rows = [r for batch in written for r in batch]
    assert len(rows) == result.events == 8
    assert sum(r["applied"] for r in rows) == result.applied == 7
