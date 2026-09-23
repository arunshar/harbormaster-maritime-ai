"""The LSN-guarded idempotent applier (Phase 2, gate C4).

Exactly-once, delivered honestly: at-least-once transport + an idempotent sink
(docs/phases/PHASE_2.md, invariants 1-5). The applier consumes a batch of
parsed messages in delivery order and enforces the commit protocol:

    apply every event -> flush every sink -> THEN call commit()

If any sink raises, commit() is never reached, the Kafka offsets stay
uncommitted, and the redelivered batch converges through the per-(table, pk)
monotonic LSN guard (duplicates and stale events no-op). Tombstones are
compaction metadata and never touch state; heartbeats and schema changes were
already typed as Skip by the envelope parser.

Ops:
    c / u     -> StateSink.upsert(table, pk, after, lsn)
    r         -> StateSink.upsert(table, pk, after, lsn=0)  (snapshot reads
                 write at a FLOOR LSN of 0: any streamed event outranks the
                 snapshot, so an update whose transaction spans the snapshot
                 consistent point is never lost, while a re-snapshot over
                 existing state still no-ops; invariant 4. The audit row keeps
                 the event's real LSN.)
    d         -> StateSink.soft_delete(table, pk, lsn)      (canonical marker;
                 no resurrection by lower-LSN stragglers; invariant 3)

Error taxonomy, deliberate: a TRANSIENT sink failure (I/O, throttling) raises
ApplyError so offsets stay put and redelivery converges; a CONTENT error
(ValueError from the key mapper: an event no mapping can ever apply) is
counted, audited as applied=False, and skipped, because a deterministic error
never converges on redelivery and one poison event must not stall every table
behind it (the same policy the parser applies to EnvelopeError).

Every data event lands in the audit sink with the guard's verdict, redeliveries
included: the audit table is transport truth, the state store is state truth,
and the difference between the two is the replay demo (invariant 5). One known
asymmetry, documented rather than hidden: state writes are durable per event
while audit rows buffer until the batch flush, so a crash mid-batch can
re-record an already-applied event as applied=False after redelivery. Exact
cross-store atomicity needs an outbox, which is out of scope; invariant 5 holds
in the absence of mid-batch crashes.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field

import structlog

from cdc.consumer.envelope import ChangeEvent, ParsedMessage, Skip, Tombstone
from cdc.sinks.base import AuditSink, EffectSink, StateSink

log = structlog.get_logger(__name__)


class ApplyError(RuntimeError):
    """A sink failed mid-batch; offsets must not be committed."""


@dataclass
class BatchResult:
    events: int = 0
    applied: int = 0
    guard_rejected: int = 0
    deletes_applied: int = 0
    tombstones: int = 0
    content_errors: int = 0
    skips: dict[str, int] = field(default_factory=dict)

    def merge_skip(self, reason: str) -> None:
        self.skips[reason] = self.skips.get(reason, 0) + 1


class Applier:
    def __init__(
        self,
        *,
        store: StateSink,
        effects: Sequence[EffectSink] = (),
        audit: AuditSink | None = None,
    ) -> None:
        self._store = store
        self._effects = tuple(effects)
        self._audit = audit

    def apply_batch(
        self, messages: Iterable[ParsedMessage], commit: Callable[[], None]
    ) -> BatchResult:
        """Apply one delivery batch, then flush, then commit. Raises ApplyError
        (offsets uncommitted) if any sink fails; redelivery converges."""
        result = BatchResult()
        try:
            for msg in messages:
                self._apply_one(msg, result)
            self._store.flush()
            for effect in self._effects:
                effect.flush()
            if self._audit is not None:
                self._audit.flush()
        except Exception as exc:
            raise ApplyError(f"sink failure mid-batch; offsets not committed: {exc}") from exc
        commit()
        return result

    def _apply_one(self, msg: ParsedMessage, result: BatchResult) -> None:
        if isinstance(msg, Skip):
            result.merge_skip(msg.reason)
            return
        if isinstance(msg, Tombstone):
            result.tombstones += 1
            return
        event: ChangeEvent = msg
        result.events += 1

        # snapshot floor: any streamed event outranks a snapshot row, so an
        # update spanning the snapshot consistent point is never lost, while a
        # re-snapshot over existing state still no-ops (invariant 4)
        guard_lsn = 0 if event.is_snapshot else event.lsn

        try:
            if event.is_delete:
                applied = self._store.soft_delete(event.table, event.pk, guard_lsn)
                if applied:
                    result.deletes_applied += 1
            else:
                applied = self._store.upsert(event.table, event.pk, event.after or {}, guard_lsn)
        except ValueError as exc:
            # a content error: no mapping can ever apply this event, and a
            # deterministic error never converges on redelivery. Count it,
            # audit it as not-applied, keep the pipeline moving.
            result.content_errors += 1
            log.error(
                "cdc_content_error_skipped",
                table=event.table,
                pk=event.pk,
                op=event.op,
                lsn=event.lsn,
                err=str(exc),
            )
            if self._audit is not None:
                self._audit.append(event, False)
            return

        if applied:
            result.applied += 1
        else:
            result.guard_rejected += 1
            log.debug(
                "cdc_guard_rejected", table=event.table, pk=event.pk, op=event.op, lsn=event.lsn
            )

        # effects fire for EVERY delivered data event, applied or not: DEL is
        # idempotent, and a guard-rejected redelivery is exactly the signal
        # that a prior attempt may have died between the store write and the
        # invalidation, so re-invalidating is the safe direction.
        for effect in self._effects:
            try:
                effect.on_change(event)
            except ValueError as exc:
                result.content_errors += 1
                log.error("cdc_effect_content_error", table=event.table, err=str(exc))

        if self._audit is not None:
            self._audit.append(event, applied)
