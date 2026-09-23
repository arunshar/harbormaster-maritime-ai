"""Iceberg cdc_audit sink (Phase 2, gate C5).

The transport-truth trail: every delivered data event appends a row,
redeliveries included, with the guard's verdict in `applied`. Replaying the
topic therefore grows the audit table (applied=false rows) while the online
state stays byte-identical; that divergence IS the acceptance demo
(docs/phases/PHASE_2.md, invariant 5).

Rows buffer in memory and land on flush(), which the applier calls before
committing offsets, so an audit row can never exist for an uncommitted batch
twice-removed: a crash before commit redelivers the batch and re-appends,
which the audit table WANTS (it records deliveries, not effects).

The writer is injected: tests use a recording writer; build_iceberg_writer()
constructs the real pyiceberg appender (SQLite catalog + file warehouse on the
local plane, Glue catalog + the S3 lake on the AWS showcase) behind the [cdc]
optional dependency.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any

import structlog

from cdc.consumer.envelope import ChangeEvent
from cdc.sinks.base import pk_key

log = structlog.get_logger(__name__)

AUDIT_TABLE = "cdc_audit"

AUDIT_FIELDS = (
    "event_table",
    "pk",
    "op",
    "lsn",
    "ts_ms",
    "before_json",
    "after_json",
    "applied",
    "consumed_at_ms",
)


def audit_row(event: ChangeEvent, applied: bool, consumed_at_ms: int) -> dict[str, Any]:
    """One audit row, exactly as delivered. Pure; golden-tested."""
    pk = event.delivered_pk if event.delivered_pk is not None else event.pk
    before = event.delivered_before if event.delivered_before is not None else event.before
    after = event.delivered_after if event.delivered_after is not None else event.after
    return {
        "event_table": event.table,
        "pk": pk_key(pk),
        "op": event.op,
        "lsn": int(event.lsn),
        "ts_ms": int(event.ts_ms),
        "before_json": None if before is None else json.dumps(before, sort_keys=True),
        "after_json": None if after is None else json.dumps(after, sort_keys=True),
        "applied": bool(applied),
        "consumed_at_ms": int(consumed_at_ms),
    }


class CdcAuditSink:
    """AuditSink: buffer rows, hand them to the writer on the batch-ack flush."""

    def __init__(
        self,
        *,
        writer: Callable[[list[dict[str, Any]]], None],
        now_ms: Callable[[], int] | None = None,
    ) -> None:
        self._writer = writer
        self._now_ms = now_ms or (lambda: int(time.time() * 1000))
        self._buffer: list[dict[str, Any]] = []

    def append(self, event: ChangeEvent, applied: bool) -> None:
        self._buffer.append(audit_row(event, applied, self._now_ms()))

    def flush(self) -> None:
        if not self._buffer:
            return
        rows, self._buffer = self._buffer, []
        try:
            self._writer(rows)
        except Exception:
            # put the rows back so the retried batch flush re-appends them;
            # the applier surfaces this as ApplyError and offsets stay put
            self._buffer = rows + self._buffer
            raise
        log.debug("cdc_audit_flushed", rows=len(rows))


def build_iceberg_writer(
    *,
    catalog_props: dict[str, str],
    namespace: str = "hm",
    table_name: str = AUDIT_TABLE,
) -> Callable[[list[dict[str, Any]]], None]:
    """The real pyiceberg appender. catalog_props examples:

    local (kind stack / drills):
        {"type": "sql",
         "uri": "sqlite:///.cdc-warehouse/catalog.db",
         "warehouse": "file://.cdc-warehouse"}
    AWS showcase (Athena-queryable):
        {"type": "glue", "warehouse": "s3://<lake-bucket>/iceberg"}
    """
    import pyarrow as pa
    from pyiceberg.catalog import load_catalog

    schema = pa.schema(
        [
            pa.field("event_table", pa.string(), nullable=False),
            pa.field("pk", pa.string(), nullable=False),
            pa.field("op", pa.string(), nullable=False),
            pa.field("lsn", pa.int64(), nullable=False),
            pa.field("ts_ms", pa.int64(), nullable=False),
            pa.field("before_json", pa.string()),
            pa.field("after_json", pa.string()),
            pa.field("applied", pa.bool_(), nullable=False),
            pa.field("consumed_at_ms", pa.int64(), nullable=False),
        ]
    )

    catalog = load_catalog("hm_audit", **catalog_props)
    try:
        catalog.create_namespace_if_not_exists(namespace)
    except AttributeError:  # older pyiceberg
        try:
            catalog.create_namespace(namespace)
        except Exception:  # nosec B110  # idempotent namespace creation, already-exists is the expected benign case
            pass
    identifier = f"{namespace}.{table_name}"
    try:
        table = catalog.load_table(identifier)
    except Exception:
        table = catalog.create_table(identifier, schema=schema)

    def writer(rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        table.append(pa.Table.from_pylist(rows, schema=schema))

    return writer
