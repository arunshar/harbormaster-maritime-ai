"""Gate C4: the applier's idempotency invariants (docs/phases/PHASE_2.md 1-5).

The heart of the phase: any delivery schedule (duplicates, restarts, shuffles)
converges to the same final online state as exactly-once delivery, offsets
commit only after every sink acks, and the audit trail records transport truth.
"""

from __future__ import annotations

import random

import pytest

from cdc.consumer.applier import Applier, ApplyError, BatchResult
from cdc.consumer.envelope import ChangeEvent, parse_envelope
from cdc.fixtures.loader import load_envelope_messages, load_expectations
from cdc.sinks.base import MemoryAudit, MemorySink


def _messages():
    return [parse_envelope(t, k, v) for t, k, v in load_envelope_messages()]


def _events() -> list[ChangeEvent]:
    return [m for m in _messages() if isinstance(m, ChangeEvent)]


def _run(messages) -> tuple[MemorySink, MemoryAudit, BatchResult, list[int]]:
    store, audit = MemorySink(), MemoryAudit()
    commits: list[int] = []
    applier = Applier(store=store, audit=audit)
    result = applier.apply_batch(messages, commit=lambda: commits.append(1))
    return store, audit, result, commits


# ------------------------------------------------------------ fixture golden


def test_fixture_final_state_matches_the_pinned_golden():
    store, _, result, commits = _run(_messages())
    exp = load_expectations()
    assert store.state_sha256() == exp["final_state_sha256"], (
        "the applied final state changed; if intentional, re-pin "
        "cdc/fixtures/expectations.json AND update PHASE_2.md in the same commit"
    )
    census = exp["apply_census"]
    assert result.events == census["events"]
    assert result.applied == census["applied"]
    assert result.guard_rejected == census["guard_rejected"]
    assert result.tombstones == census["tombstones"]
    assert commits == [1]


def test_fixture_semantics_spot_checks():
    store, _, _, _ = _run(_messages())
    state = store.final_state()
    events = _events()
    update_lsn = [e.lsn for e in events if e.op == "u"][0]
    delete_lsn = [e.lsn for e in events if e.op == "d"][0]
    # the update won over the create and its redelivery
    wl = state['watchlist|{"mmsi":200000003,"tenant_id":"00000000-0000-0000-0000-000000000000"}']
    assert wl["deleted"] is False and wl["row"]["severity"] == 0.95
    assert wl["last_applied_lsn"] == update_lsn
    # the snapshot-seeded watchlist row was deleted by the streamed op=d
    legacy = state[
        'watchlist|{"mmsi":200000001,"tenant_id":"00000000-0000-0000-0000-000000000000"}'
    ]
    assert legacy["deleted"] is True and legacy["row"] is None
    assert legacy["last_applied_lsn"] == delete_lsn
    # snapshot vessels row survives untouched
    assert (
        state['vessels|{"mmsi":200000001,"tenant_id":"00000000-0000-0000-0000-000000000000"}'][
            "row"
        ]["name"]
        == "HM DEMO VESSEL A"
    )


# ------------------------------------------------------- invariants 1 and 2


def test_full_replay_is_a_no_op_on_state():
    msgs = _messages()
    store, audit, _, _ = _run(msgs)
    first_hash = store.state_sha256()

    applier = Applier(store=store, audit=audit)
    result2 = applier.apply_batch(msgs, commit=lambda: None)
    assert store.state_sha256() == first_hash
    assert result2.applied == 0
    assert result2.guard_rejected == result2.events  # every redelivery rejected


def test_equal_lsn_redelivery_is_rejected_not_reapplied():
    e = _events()[0]
    store = MemorySink()
    assert store.upsert(e.table, e.pk, e.after or {}, e.lsn) is True
    assert store.upsert(e.table, e.pk, e.after or {}, e.lsn) is False  # equal, not >


def test_out_of_order_older_lsn_no_ops():
    store = MemorySink()
    pk = {"mmsi": 1}
    assert store.upsert("watchlist", pk, {"severity": 0.95}, lsn=3000) is True
    assert store.upsert("watchlist", pk, {"severity": 0.9}, lsn=2000) is False
    assert store.final_state()['watchlist|{"mmsi":1}']["row"]["severity"] == 0.95


# ------------------------------------------------------------- invariant 3


def test_delete_then_replayed_older_update_stays_deleted():
    store = MemorySink()
    pk = {"mmsi": 1}
    store.upsert("watchlist", pk, {"reason": "x"}, lsn=2000)
    assert store.soft_delete("watchlist", pk, lsn=6000) is True
    assert store.upsert("watchlist", pk, {"reason": "x"}, lsn=2000) is False  # no resurrection
    item = store.final_state()['watchlist|{"mmsi":1}']
    assert item["deleted"] is True and item["row"] is None


def test_delete_marker_is_canonical_regardless_of_arrival_order():
    in_order, shuffled = MemorySink(), MemorySink()
    pk = {"mmsi": 1}
    in_order.upsert("watchlist", pk, {"reason": "x"}, lsn=1000)
    in_order.soft_delete("watchlist", pk, lsn=6000)
    shuffled.soft_delete("watchlist", pk, lsn=6000)  # delete arrives first
    shuffled.upsert("watchlist", pk, {"reason": "x"}, lsn=1000)  # straggler rejected
    assert in_order.state_sha256() == shuffled.state_sha256()


# ------------------------------------------------------------- invariant 4


def _mk(op: str, lsn: int, after=None, pk=None) -> ChangeEvent:
    return ChangeEvent(
        table="watchlist",
        pk=pk or {"mmsi": 1},
        op=op,
        lsn=lsn,
        ts_ms=0,
        before=None,
        after=after if after is not None else {"reason": "x"},
    )


def test_snapshot_writes_at_the_floor_and_stream_always_outranks():
    """Snapshot rows apply at guard LSN 0: a streamed change whose transaction
    spanned the snapshot consistent point (source.lsn below the snapshot LSN)
    still applies, so no update is lost; a re-snapshot no-ops over existing
    state and cannot clobber streamed data (invariant 4)."""
    store = MemorySink()
    applier = Applier(store=store, audit=MemoryAudit())

    # snapshot seeds at floor 0 even though its real LSN is high
    r1 = applier.apply_batch([_mk("r", 5000)], commit=lambda: None)
    assert r1.applied == 1
    assert store.final_state()['watchlist|{"mmsi":1}']["last_applied_lsn"] == 0

    # a spanning-transaction stream event with a LOWER source.lsn still wins
    r2 = applier.apply_batch([_mk("u", 4000, after={"reason": "y"})], commit=lambda: None)
    assert r2.applied == 1
    assert store.final_state()['watchlist|{"mmsi":1}']["row"]["reason"] == "y"

    # a re-snapshot after the stream cannot clobber streamed state
    r3 = applier.apply_batch([_mk("r", 9000)], commit=lambda: None)
    assert r3.applied == 0 and r3.guard_rejected == 1
    assert store.final_state()['watchlist|{"mmsi":1}']["row"]["reason"] == "y"

    # and a duplicate snapshot into an otherwise-empty key applies exactly once
    store2 = MemorySink()
    a2 = Applier(store=store2, audit=MemoryAudit())
    rr = a2.apply_batch([_mk("r", 5000), _mk("r", 5000)], commit=lambda: None)
    assert rr.applied == 1 and rr.guard_rejected == 1


def test_p39_resnapshot_requires_cleared_tenant_qualified_state():
    pk = {"tenant_id": "00000000-0000-0000-0000-000000000000", "mmsi": 1}
    store = MemorySink()
    assert store.upsert("watchlist", pk, {"reason": "stale"}, lsn=7) is True

    result = Applier(store=store, audit=MemoryAudit()).apply_batch(
        [_mk("r", 9000, after={"reason": "snapshot"}, pk=pk)],
        commit=lambda: None,
    )

    assert result.applied == 0 and result.guard_rejected == 1
    state = store.final_state()[
        'watchlist|{"mmsi":1,"tenant_id":"00000000-0000-0000-0000-000000000000"}'
    ]
    assert state["row"]["reason"] == "stale" and state["last_applied_lsn"] == 7


def test_content_error_is_counted_audited_and_never_stalls_the_batch():
    """A poison event (no key mapping can ever apply it) must not block the
    partition: counted, audited applied=False, batch still commits."""

    class PoisonStore(MemorySink):
        def upsert(self, table, pk, row, lsn):
            if pk == {"id": "200:"}:
                raise ValueError("sanctions_flags id is not 'mmsi:regime'")
            return super().upsert(table, pk, row, lsn)

    poison = ChangeEvent(
        table="sanctions_flags",
        pk={"id": "200:"},
        op="c",
        lsn=7000,
        ts_ms=0,
        before=None,
        after={"id": "200:"},
    )
    committed: list[int] = []
    store, audit = PoisonStore(), MemoryAudit()
    result = Applier(store=store, audit=audit).apply_batch(
        [poison, _mk("c", 8000)], commit=lambda: committed.append(1)
    )
    assert result.content_errors == 1
    assert result.applied == 1  # the healthy event behind the poison one landed
    assert committed == [1]  # offsets advanced; no crash loop
    assert [r["applied"] for r in audit.rows] == [False, True]


# ------------------------------------------------- commit protocol (inv. 2)


class _FailingSink(MemorySink):
    def flush(self) -> None:
        raise RuntimeError("sink outage")


def test_commit_fires_only_after_all_sinks_flush():
    order: list[str] = []

    class OrderedAudit(MemoryAudit):
        def flush(self) -> None:
            order.append("audit_flush")
            super().flush()

    class OrderedStore(MemorySink):
        def flush(self) -> None:
            order.append("store_flush")
            super().flush()

    applier = Applier(store=OrderedStore(), audit=OrderedAudit())
    applier.apply_batch(_messages(), commit=lambda: order.append("commit"))
    assert order == ["store_flush", "audit_flush", "commit"]


def test_sink_failure_leaves_offsets_uncommitted_and_redelivery_converges():
    committed: list[int] = []
    failing = _FailingSink()
    applier = Applier(store=failing, audit=MemoryAudit())
    with pytest.raises(ApplyError):
        applier.apply_batch(_messages(), commit=lambda: committed.append(1))
    assert committed == []

    # the crash-recovery path: a fresh consumer redelivers the whole batch into
    # the SAME store state; the guard absorbs everything already applied
    good = MemorySink()
    Applier(store=good, audit=MemoryAudit()).apply_batch(_messages(), commit=lambda: None)
    recovered = MemorySink()
    recovered._items = failing._items  # state survived the crash
    Applier(store=recovered, audit=MemoryAudit()).apply_batch(_messages(), commit=lambda: None)
    assert recovered.state_sha256() == good.state_sha256()


# ---------------------------------------------------------------- effects


def test_effect_sink_fires_for_every_delivered_data_event():
    """Effects are idempotent and fire for guard-rejected redeliveries too: a
    rejected redelivery is the signal a prior attempt may have died between
    the store write and the invalidation."""
    fired: list[tuple[str, int]] = []

    class RecordingEffect:
        def on_change(self, event: ChangeEvent) -> None:
            fired.append((event.table, event.lsn))

        def flush(self) -> None:
            return None

    store = MemorySink()
    applier = Applier(store=store, effects=(RecordingEffect(),), audit=MemoryAudit())
    result = applier.apply_batch(_messages(), commit=lambda: None)
    create_lsn = [e.lsn for e in _events() if e.op == "c" and e.table == "watchlist"][0]
    assert len(fired) == result.events  # applied AND guard-rejected
    assert fired.count(("watchlist", create_lsn)) == 2  # the redelivered dupe re-fires


# ------------------------------------------------------------------- audit


def test_audit_records_transport_truth_including_redeliveries():
    _, audit, result, _ = _run(_messages())
    create_lsn = [e.lsn for e in _events() if e.op == "c" and e.table == "watchlist"][0]
    assert len(audit.rows) == result.events  # every data event, dupes included
    assert sum(r["applied"] for r in audit.rows) == result.applied
    dupes = [r for r in audit.rows if r["lsn"] == create_lsn and r["event_table"] == "watchlist"]
    assert [r["applied"] for r in dupes] == [True, False]  # first applied, replay rejected
    assert {r["pk"] for r in dupes} == {'{"mmsi":200000003}'}


def test_audit_buffers_until_flush():
    audit = MemoryAudit()
    e = _events()[0]
    audit.append(e, True)
    assert audit.rows == []  # buffered
    audit.flush()
    assert len(audit.rows) == 1


# ----------------------------------------------- convergence (the property)


def test_any_delivery_schedule_converges_to_the_same_state():
    """Shuffles, duplications, and partial-restart replays all converge."""
    baseline, _, _, _ = _run(_messages())
    expected = baseline.state_sha256()
    events = _events()

    rng = random.Random(20260703)
    for _ in range(25):
        schedule: list[ChangeEvent] = list(events)
        # duplicate a random slice (an at-least-once redelivery window)
        i = rng.randrange(len(events))
        j = rng.randrange(i, len(events))
        schedule.extend(events[i : j + 1])
        # a consumer restart replaying from a random earlier offset
        schedule.extend(events[rng.randrange(len(events)) :])
        rng.shuffle(schedule)

        store = MemorySink()
        Applier(store=store, audit=MemoryAudit()).apply_batch(schedule, commit=lambda: None)
        assert store.state_sha256() == expected
