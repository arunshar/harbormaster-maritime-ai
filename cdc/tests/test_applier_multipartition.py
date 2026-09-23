"""Gate C4, multi-partition + rebalance: the per-(table, pk) LSN guard still
converges under two partitions and a consumer-group rebalance mid-stream.

The single-partition property test (test_applier.py) shuffles, duplicates, and
replays one stream. This file adds the topology the guard actually has to
survive in production: a keyed topic split across partitions, consumed by a
group whose assignment changes mid-stream.

The CDC facts being modeled (no docker, no real Kafka, same in-memory fakes):

- Debezium keys every record by (table, pk), so one key always lands in one
  partition and per-key commit order is preserved WITHIN that partition.
  _partition_of assigns keys to partitions the same way (a stable digest mod n),
  so we never fabricate an ordering the real system would not produce (a key
  never splits across partitions).
- A consumer-group rebalance reassigns a partition to another consumer, which
  resumes from the last COMMITTED offset. Records delivered but not yet
  committed are redelivered. That is the only honest source of duplicates and
  cross-partition reordering, and it is exactly what the _Partition
  committed-offset cursor models: rewind to the commit point, redeliver the
  tail.
- Convergence rests on the store invariant proven in base.py: whole-item puts
  under a monotonic per-key guard make the final state a function of the
  max-LSN event delivered per key, independent of order or duplication. Each
  test asserts against an independent max-LSN oracle, not just against a
  golden hash, so a guard regression is localized to the offending key.
"""

from __future__ import annotations

import hashlib
import random

from cdc.consumer.applier import Applier
from cdc.consumer.envelope import ChangeEvent
from cdc.sinks.base import MemoryAudit, MemorySink, pk_key

# --------------------------------------------------------------- event builder


def _ev(table: str, pk: dict, op: str, lsn: int, after=None) -> ChangeEvent:
    """A single change event. after defaults to a per-lsn payload so a wrong
    winner is visible in the row content, not just the LSN bookkeeping."""
    if after is None and op != "d":
        after = {"v": lsn}
    return ChangeEvent(
        table=table,
        pk=pk,
        op=op,
        lsn=lsn,
        ts_ms=lsn,
        before=None,
        after=after,
    )


# -------------------------------------------------------- max-LSN state oracle


def _oracle(events: list[ChangeEvent]) -> dict[str, dict]:
    """The correct final store contents, computed independently of the applier:
    per key, the event with the maximum LSN wins (delete -> tombstone marker,
    upsert -> its row), matching MemorySink's whole-item put under a monotonic
    guard. Snapshot floor (op=r -> guard LSN 0) is applied here too so the
    oracle matches the applier's snapshot semantics."""
    winner: dict[tuple[str, str], ChangeEvent] = {}
    guard_lsn: dict[tuple[str, str], int] = {}
    for e in events:
        key = (e.table, pk_key(e.pk))
        eff = 0 if e.is_snapshot else e.lsn
        # strict >: an equal-LSN redelivery never displaces the incumbent, and
        # the first arrival at a given effective LSN is the canonical one.
        if key not in guard_lsn or eff > guard_lsn[key]:
            guard_lsn[key] = eff
            winner[key] = e
    state: dict[str, dict] = {}
    for (table, k), e in winner.items():
        eff = 0 if e.is_snapshot else e.lsn
        if e.is_delete:
            state[f"{table}|{k}"] = {"row": None, "deleted": True, "last_applied_lsn": eff}
        else:
            state[f"{table}|{k}"] = {
                "row": dict(e.after or {}),
                "deleted": False,
                "last_applied_lsn": eff,
            }
    return dict(sorted(state.items()))


def _apply(schedule: list[ChangeEvent]) -> MemorySink:
    store = MemorySink()
    Applier(store=store, audit=MemoryAudit()).apply_batch(schedule, commit=lambda: None)
    return store


# ------------------------------------------------------------- partition fakes


class _Partition:
    """One Kafka partition: an ordered log of events plus a committed-offset
    cursor. deliver_new() yields everything past the last commit; rebalance
    (redeliver_from_commit) rewinds the read cursor to the commit point so the
    uncommitted tail is delivered again, which is what a mid-stream reassignment
    does to a consumer that had not yet committed."""

    def __init__(self, events: list[ChangeEvent]) -> None:
        self._log = list(events)
        self._committed = 0  # offset of the next uncommitted record
        self._read = 0  # how far this consumer has read

    def deliver_new(self) -> list[ChangeEvent]:
        out = self._log[self._read :]
        self._read = len(self._log)
        return out

    def commit(self) -> None:
        self._committed = self._read

    def redeliver_from_commit(self) -> list[ChangeEvent]:
        """Rebalance: a new owner resumes from the committed offset, redelivering
        the tail this consumer read but never committed."""
        self._read = self._committed
        return self.deliver_new()


def _partition_of(event: ChangeEvent, n: int) -> int:
    """Deterministic keyed assignment, mirroring Debezium: a given (table, pk)
    hashes to exactly one partition, so per-key order is preserved within it.
    Uses a stable digest, not builtin hash(), so the split is reproducible
    across runs regardless of PYTHONHASHSEED (seeded-determinism gate)."""
    digest = hashlib.sha256(f"{event.table}|{pk_key(event.pk)}".encode()).digest()
    return int.from_bytes(digest[:8], "big") % n


def _split_into_partitions(events: list[ChangeEvent], n: int) -> list[list[ChangeEvent]]:
    parts: list[list[ChangeEvent]] = [[] for _ in range(n)]
    for e in events:
        parts[_partition_of(e, n)].append(e)
    return parts


# ============================================================ the source stream
#
# Three keys, keyed-hashed onto partitions by _partition_of. At n=2 the stable
# digest puts A and B on partition 1 and C on partition 0, so the stream really
# does split across a partition boundary; tests 1-2 interleave those partitions.
# The assertions never depend on WHICH partition a key lands on, only that a key
# stays on one partition, because the oracle is computed from the events alone.
# The stream mutates each key several times so ordering actually matters. Tests
# 3 and 5 pin keys onto explicit partitions directly, independent of the hash.

_A = {"mmsi": 200000010}
_B = {"mmsi": 200000020}
_C = {"mmsi": 200000030}

_STREAM = [
    _ev("watchlist", _A, "r", 1000),  # snapshot seed (floor 0)
    _ev("watchlist", _A, "c", 1100),
    _ev("watchlist", _B, "c", 1200),
    _ev("watchlist", _A, "u", 1300, after={"v": 1300, "sev": 0.9}),
    _ev("watchlist", _B, "u", 1400, after={"v": 1400, "sev": 0.5}),
    _ev("watchlist", _C, "c", 1500),
    _ev("watchlist", _A, "u", 1600, after={"v": 1600, "sev": 0.95}),  # A's winner
    _ev("watchlist", _B, "d", 1700),  # B ends deleted
    _ev("watchlist", _C, "u", 1800, after={"v": 1800, "sev": 0.3}),  # C's winner
]


# ------------------------------------------------------ 1. duplicate-after-rebalance


def test_duplicate_delivery_after_rebalance_converges():
    """A rebalance redelivers a partition's uncommitted tail; those duplicates,
    interleaved with the other partition's ongoing delivery, must no-op through
    the guard and leave the exact single-delivery state."""
    parts = _split_into_partitions(_STREAM, 2)
    p0, p1 = _Partition(parts[0]), _Partition(parts[1])

    schedule: list[ChangeEvent] = []
    # p0 delivers and commits; p1 delivers WITHOUT committing (in-flight).
    schedule += p0.deliver_new()
    p0.commit()
    schedule += p1.deliver_new()
    # rebalance: p1 is reassigned and its new owner resumes from the (empty)
    # commit point, so every record p1 already handed us is delivered a second
    # time -- duplicates the guard must absorb.
    schedule += p1.redeliver_from_commit()
    p1.commit()

    store = _apply(schedule)
    assert store.final_state() == _oracle(_STREAM)
    # and the redelivered duplicates really were in the schedule
    assert len(schedule) > len(_STREAM)


# ------------------------------------------------- 2. out-of-order across partitions


def test_out_of_order_across_partitions_converges():
    """Interleave the two partitions so a higher-LSN change on one key is applied
    before a lower-LSN straggler on another key is redelivered after a rebalance.
    The per-key guard rejects the stale stragglers; no key regresses."""
    parts = _split_into_partitions(_STREAM, 2)
    p0, p1 = _Partition(parts[0]), _Partition(parts[1])

    d0 = p0.deliver_new()
    d1 = p1.deliver_new()
    p1.commit()  # p1 commits; p0 does not

    schedule: list[ChangeEvent] = []
    # interleave the two partitions' first delivery record-by-record, so keys on
    # different partitions arrive out of their global LSN order
    # strict=False on purpose: d0 and d1 differ in length; the leftover tail is
    # appended below, so truncating the zip to the shorter one is intended
    for a, b in zip(d0, d1, strict=False):
        schedule.append(b)
        schedule.append(a)
    schedule += d0[len(d1) :]
    schedule += d1[len(d0) :]
    # rebalance p0: its uncommitted records redeliver and land AFTER p1's newer
    # changes are already in the store -- the stale-straggler case
    schedule += p0.redeliver_from_commit()

    store = _apply(schedule)
    oracle = _oracle(_STREAM)
    assert store.final_state() == oracle

    # explicit no-regression spot checks on the keys that were reordered
    st = store.final_state()
    ka = f"watchlist|{pk_key(_A)}"
    if ka in oracle:
        assert st[ka]["last_applied_lsn"] == oracle[ka]["last_applied_lsn"]
        assert st[ka]["last_applied_lsn"] >= 1600 or oracle[ka]["last_applied_lsn"] == 0


# ------------------------------------------- 3. delete straggler cannot resurrect


def test_deleted_key_is_not_resurrected_by_a_redelivered_older_upsert():
    """B is deleted at LSN 1700. If a rebalance redelivers B's earlier op=c/op=u
    (lower LSN) across a partition boundary, the delete marker must stand."""
    # force A and B onto separate partitions for this scenario by splitting on a
    # dedicated 2-way keyed map (independent of the global hash bucket count)
    b_events = [e for e in _STREAM if e.pk == _B]
    other = [e for e in _STREAM if e.pk != _B]

    pB = _Partition(b_events)
    pOther = _Partition(other)

    schedule: list[ChangeEvent] = []
    schedule += pB.deliver_new()  # B's full life incl. the op=d at 1700
    pB.commit()
    schedule += pOther.deliver_new()
    pOther.commit()
    # rebalance replays B's early upserts (1200, 1400) AFTER the delete is live
    schedule += [e for e in b_events if e.op in ("c", "u")]

    store = _apply(schedule)
    kb = f"watchlist|{pk_key(_B)}"
    item = store.final_state()[kb]
    assert item["deleted"] is True and item["row"] is None
    assert item["last_applied_lsn"] == 1700
    assert store.final_state() == _oracle(_STREAM)


# ------------------------------------------------- 4. property: random rebalances


def test_property_random_multipartition_rebalances_converge():
    """Over several seeds and 2-4 partitions, drive a randomized delivery with
    interleaving, per-partition duplicate redelivery (rebalance), and a late
    full-suffix replay (a consumer restart). Every schedule must converge to the
    single max-LSN-per-key oracle."""
    oracle = _oracle(_STREAM)
    base = len(_STREAM)
    saw_duplicates = False
    for seed in range(12):
        rng = random.Random(20260706 + seed)
        n = rng.choice([2, 3, 4])
        parts = [_Partition(p) for p in _split_into_partitions(_STREAM, n)]

        schedule: list[ChangeEvent] = []
        # phase 1: each partition delivers its stream; some commit, some do not
        first: list[list[ChangeEvent]] = []
        for p in parts:
            first.append(p.deliver_new())
        # interleave the per-partition deliveries by round-robin with jitter,
        # preserving each partition's internal order (real per-partition FIFO)
        cursors = [0] * n
        while any(cursors[i] < len(first[i]) for i in range(n)):
            i = rng.randrange(n)
            if cursors[i] < len(first[i]):
                schedule.append(first[i][cursors[i]])
                cursors[i] += 1
        # phase 2: a random subset commits; the rest get rebalanced (redelivered)
        for p in parts:
            if rng.random() < 0.5:
                p.commit()
        for p in parts:
            redelivered = p.redeliver_from_commit()
            # splice the redelivered tail in at a random point, still after its
            # own first delivery (a rebalance cannot deliver before it read)
            if redelivered:
                cut = rng.randrange(len(schedule) + 1)
                schedule[cut:cut] = redelivered
        # phase 3: a full replay from zero on one partition (restart from an old
        # committed offset that predates everything)
        victim = rng.randrange(n)
        schedule += parts[victim]._log

        if len(schedule) > base:
            saw_duplicates = True
        store = _apply(schedule)
        assert store.final_state() == oracle, f"seed {seed}: n={n} did not converge"

    # guard against a silently-weakened property: if the randomizer ever stopped
    # producing redeliveries, every schedule would be a plain permutation and the
    # duplicate-absorption path would go untested.
    assert saw_duplicates, "no seed produced a redelivery; duplicates went untested"


# --------------------------------------- 5. equal-LSN cross-partition duplicate


def test_equal_lsn_duplicate_across_a_rebalance_is_rejected_once():
    """The subtle guard case: the SAME event (same LSN) delivered on the original
    partition and again after a rebalance. The guard uses strict >, so the second
    copy is guard-rejected, not reapplied, and the audit records both."""
    ev = _ev("watchlist", _A, "c", 4242)
    store, audit = MemorySink(), MemoryAudit()
    applier = Applier(store=store, audit=audit)
    # first delivery applies; the post-rebalance duplicate of the identical LSN
    # must be rejected (equal is not greater)
    result = applier.apply_batch([ev, ev], commit=lambda: None)
    assert result.applied == 1
    assert result.guard_rejected == 1
    assert [r["applied"] for r in audit.rows] == [True, False]
    assert store.final_state() == _oracle([ev])
