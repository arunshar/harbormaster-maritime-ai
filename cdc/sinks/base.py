"""Sink contracts + in-memory reference implementations (Phase 2, gates C4/C5).

Three sink roles, matching docs/phases/PHASE_2.md:

- StateSink: the guarded online store. upsert/soft_delete carry the event LSN
  and return whether the write APPLIED (True) or was GUARD-REJECTED (False,
  a stale or duplicate delivery). The guard is per (table, pk) and monotonic:
  apply only if lsn > last_applied_lsn. Writes are WHOLE-ITEM puts, never
  attribute merges; combined with the monotonic guard this makes the final
  state a function of the max-LSN event delivered per key, so any delivery
  order with any duplication converges to the same state (the property the
  gate-C4 tests assert).
- EffectSink: idempotent side effects fired for EVERY delivered data event,
  applied or guard-rejected (Redis invalidation: a rejected redelivery is the
  signal that a prior attempt may have died between the store write and the
  invalidation, so re-firing is the safe direction). Not a state store; it has
  no guard of its own, so its operations must be idempotent.
- AuditSink: the transport-level trail. Every data event appends, redeliveries
  included, with the guard's verdict in `applied`.

MemorySink implements the exact semantics of the DynamoDB conditional
expression (cdc/sinks/dynamo.py), so the applier's tests are
sink-implementation-independent; a gate-C5 test holds the two in lockstep.

Soft deletes write a canonical marker {deleted: true, last_applied_lsn} with
the row content dropped, so a delete applied before a straggling lower-LSN
upsert leaves the same bytes as the in-order sequence (no resurrection,
invariant 3).
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Protocol

from cdc.consumer.envelope import ChangeEvent


def pk_key(pk: dict[str, Any]) -> str:
    """Canonical string form of a primary key dict (order-insensitive)."""
    return json.dumps(pk, sort_keys=True, separators=(",", ":"))


class StateSink(Protocol):
    def upsert(self, table: str, pk: dict[str, Any], row: dict[str, Any], lsn: int) -> bool: ...
    def soft_delete(self, table: str, pk: dict[str, Any], lsn: int) -> bool: ...
    def flush(self) -> None: ...


class EffectSink(Protocol):
    def on_change(self, event: ChangeEvent) -> None: ...
    def flush(self) -> None: ...


class AuditSink(Protocol):
    def append(self, event: ChangeEvent, applied: bool) -> None: ...
    def flush(self) -> None: ...


class MemorySink:
    """Reference StateSink: dict-backed, DynamoDB-conditional-expression semantics."""

    def __init__(self) -> None:
        self._items: dict[tuple[str, str], dict[str, Any]] = {}
        self.flushes = 0

    def _guard_passes(self, key: tuple[str, str], lsn: int) -> bool:
        existing = self._items.get(key)
        # attribute_not_exists(last_applied_lsn) OR last_applied_lsn < :lsn
        return existing is None or existing["last_applied_lsn"] < lsn

    def upsert(self, table: str, pk: dict[str, Any], row: dict[str, Any], lsn: int) -> bool:
        key = (table, pk_key(pk))
        if not self._guard_passes(key, lsn):
            return False
        self._items[key] = {"row": dict(row), "deleted": False, "last_applied_lsn": lsn}
        return True

    def soft_delete(self, table: str, pk: dict[str, Any], lsn: int) -> bool:
        key = (table, pk_key(pk))
        if not self._guard_passes(key, lsn):
            return False
        self._items[key] = {"row": None, "deleted": True, "last_applied_lsn": lsn}
        return True

    def flush(self) -> None:
        self.flushes += 1

    # ------------------------------------------------------------- test API

    def final_state(self) -> dict[str, dict[str, Any]]:
        return {f"{t}|{k}": dict(v) for (t, k), v in sorted(self._items.items())}

    def state_sha256(self) -> str:
        canon = json.dumps(self.final_state(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canon.encode()).hexdigest()


class MemoryAudit:
    """Reference AuditSink: buffered like the Iceberg sink, flushed on batch ack."""

    def __init__(self) -> None:
        self._buffer: list[dict[str, Any]] = []
        self.rows: list[dict[str, Any]] = []

    def append(self, event: ChangeEvent, applied: bool) -> None:
        delivered_pk = event.delivered_pk if event.delivered_pk is not None else event.pk
        self._buffer.append(
            {
                "event_table": event.table,
                "pk": pk_key(delivered_pk),
                "op": event.op,
                "lsn": event.lsn,
                "applied": applied,
            }
        )

    def flush(self) -> None:
        self.rows.extend(self._buffer)
        self._buffer.clear()
