"""Debezium envelope parsing (Phase 2, gate C3).

Turns raw Kafka (key, value) byte pairs from the Debezium Postgres connector
into typed ChangeEvents the applier consumes. Handles the four data ops
(c=create, u=update, d=delete, r=snapshot read), the post-delete Kafka
tombstone (null value; compaction metadata, not a data event), heartbeat
messages, schema-change messages, and both unwrapped and schema-wrapped
(`{"schema": ..., "payload": ...}`) JSON converter output.

The LSN comes from source.lsn (pgoutput); it is the total order the applier's
monotonic guard runs on, so a missing LSN is a hard parse error, never a
default. Snapshot reads carry the snapshot LSN, which is what makes the
snapshot-to-stream transition duplicate-safe under the same guard
(docs/phases/PHASE_2.md, invariant 4).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from cdc.schema.ddl import CDC_TABLES, DEFAULT_TENANT_ID

DATA_OPS = ("c", "u", "d", "r")


class EnvelopeError(ValueError):
    """The message is not a well-formed Debezium data envelope."""


@dataclass(frozen=True)
class ChangeEvent:
    """One row-level change, in commit order per (table, pk)."""

    table: str
    pk: dict[str, Any]
    op: str  # c | u | d | r
    lsn: int
    ts_ms: int
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    source: dict[str, Any] = field(default_factory=dict, hash=False)
    # Routing normalization may add tenant_id to legacy row images. Retain the
    # delivered key and images separately so cdc_audit remains transport truth.
    delivered_pk: dict[str, Any] | None = field(default=None, compare=False, hash=False, repr=False)
    delivered_before: dict[str, Any] | None = field(
        default=None, compare=False, hash=False, repr=False
    )
    delivered_after: dict[str, Any] | None = field(
        default=None, compare=False, hash=False, repr=False
    )

    @property
    def is_delete(self) -> bool:
        return self.op == "d"

    @property
    def is_snapshot(self) -> bool:
        return self.op == "r"


@dataclass(frozen=True)
class Tombstone:
    """The null-value compaction marker Debezium emits after a delete. Skipped
    by the applier: the op=d event that precedes it is the data-bearing delete."""

    pk: dict[str, Any]


@dataclass(frozen=True)
class Skip:
    """A non-data message (heartbeat, schema change). Counted, never applied."""

    reason: str


ParsedMessage = ChangeEvent | Tombstone | Skip

HEARTBEAT_TOPIC_PREFIX = "__debezium-heartbeat"


def _unwrap(raw: bytes | str | None) -> dict[str, Any] | None:
    """Decode JSON and unwrap the {"schema", "payload"} converter envelope."""
    if raw is None:
        return None
    try:
        obj = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise EnvelopeError(f"message is not JSON: {exc}") from exc
    if obj is None:
        return None
    if not isinstance(obj, dict):
        raise EnvelopeError(f"message is not a JSON object: {type(obj).__name__}")
    if set(obj.keys()) == {"schema", "payload"} or (
        "payload" in obj and "schema" in obj and isinstance(obj["payload"], dict | type(None))
    ):
        return obj["payload"]
    return obj


def _canonical_tenant_id(value: Any) -> str:
    try:
        return str(UUID(str(value)))
    except (ValueError, TypeError, AttributeError) as exc:
        raise EnvelopeError(f"invalid tenant_id: {value!r}") from exc


def _normalize_registry_identity(
    key_obj: dict[str, Any], payload: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Converge legacy and composite Debezium keys on one tenant identity.

    Events recorded before P39 have no tenant column and belong to the legacy
    single-tenant sentinel. During a schema transition, a row tenant can also
    arrive with the old key shape. Any explicit disagreement is corruption and
    fails before a derived-store write.
    """
    explicit: set[str] = set()
    if "tenant_id" in key_obj:
        explicit.add(_canonical_tenant_id(key_obj["tenant_id"]))
    for image_name in ("before", "after"):
        image = payload.get(image_name)
        if isinstance(image, dict) and "tenant_id" in image:
            explicit.add(_canonical_tenant_id(image["tenant_id"]))
    if len(explicit) > 1:
        raise EnvelopeError("envelope has mismatched tenant_id in key and row")
    tenant_id = next(iter(explicit), DEFAULT_TENANT_ID)

    normalized_key = dict(key_obj)
    normalized_key["tenant_id"] = tenant_id
    normalized_payload = dict(payload)
    for image_name in ("before", "after"):
        image = payload.get(image_name)
        if isinstance(image, dict):
            normalized_image = dict(image)
            normalized_image["tenant_id"] = tenant_id
            normalized_payload[image_name] = normalized_image
    return normalized_key, normalized_payload


def parse_envelope(topic: str, key: bytes | str | None, value: bytes | str | None) -> ParsedMessage:
    """Parse one Kafka message from a Debezium topic into a typed event."""
    if topic.startswith(HEARTBEAT_TOPIC_PREFIX):
        return Skip("heartbeat")

    key_obj = _unwrap(key) or {}
    payload = _unwrap(value)

    if payload is None:
        # Kafka tombstone: null value after an op=d event (tombstones.on.delete).
        if key_obj:
            tenant_id = _canonical_tenant_id(key_obj.get("tenant_id", DEFAULT_TENANT_ID))
            key_obj = {**key_obj, "tenant_id": tenant_id}
        return Tombstone(pk=key_obj)

    if "op" not in payload:
        if "ddl" in payload or "tableChanges" in payload:
            return Skip("schema_change")
        raise EnvelopeError(f"envelope has no op and is not a schema change: {sorted(payload)}")

    op = payload["op"]
    if op not in DATA_OPS:
        raise EnvelopeError(f"unknown op {op!r}")

    source = payload.get("source") or {}
    table = source.get("table")
    if not table:
        raise EnvelopeError("envelope source has no table")
    lsn = source.get("lsn")
    if lsn is None:
        raise EnvelopeError(f"envelope for {table} has no source.lsn; the guard needs it")

    if not key_obj:
        raise EnvelopeError(f"envelope for {table} has no key; the pk comes from the key")
    delivered_pk = dict(key_obj)
    delivered_before = dict(payload["before"]) if isinstance(payload.get("before"), dict) else None
    delivered_after = dict(payload["after"]) if isinstance(payload.get("after"), dict) else None
    if table in CDC_TABLES:
        key_obj, payload = _normalize_registry_identity(key_obj, payload)

    return ChangeEvent(
        table=str(table),
        pk=key_obj,
        op=op,
        lsn=int(lsn),
        ts_ms=int(payload.get("ts_ms", 0)),
        before=payload.get("before"),
        after=payload.get("after"),
        source=source,
        delivered_pk=delivered_pk,
        delivered_before=delivered_before,
        delivered_after=delivered_after,
    )
