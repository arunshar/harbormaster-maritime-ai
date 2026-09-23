"""DynamoDB online-store sink (Phase 2, gate C5).

Writes the CDC-fed online state into the Phase 0 feast_online table layout
(hash entity_id S, range feature_name S; docs/phases/PHASE_2.md decisions):

    vessels          -> (str(mmsi), "vessel_meta")
    watchlist        -> (str(mmsi), "watchlist")
    sanctions_flags  -> (str(mmsi), "sanctions:<regime>")   from the "mmsi:regime" id

The idempotency guard lives in the ConditionExpression: a whole-item PutItem
that applies only when the incoming LSN is strictly newer. Guard rejection
(ConditionalCheckFailedException) means a stale or duplicate delivery and maps
to applied=False, never an error. Whole-item puts, not attribute merges: the
final item is a function of the max-LSN event per key, which is what makes any
delivery order converge (see cdc/sinks/base.py).

put_item is issued per event because BatchWriteItem does not support condition
expressions; registry change volume is analyst-scale, so this is fine.

The key vocabulary (feature names, tenant-qualified entity id) is duplicated from
serving/app/watchlist.py on purpose: the serving wheel is not a cdc dependency.
A test in cdc/tests/test_sinks.py asserts the two stay identical.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import structlog

log = structlog.get_logger(__name__)

# Duplicated from serving/app/watchlist.py; drift-guarded by test_sinks.py.
FEATURE_VESSEL_META = "vessel_meta"
FEATURE_WATCHLIST = "watchlist"
FEATURE_SANCTIONS_PREFIX = "sanctions:"

TABLE_FEATURES = {"vessels": FEATURE_VESSEL_META, "watchlist": FEATURE_WATCHLIST}
SANCTIONS_TABLE = "sanctions_flags"

CONDITION_EXPRESSION = "attribute_not_exists(last_applied_lsn) OR last_applied_lsn < :lsn"


def _tenant_id(pk: dict[str, Any], table: str) -> str:
    tenant_id = pk.get("tenant_id")
    if tenant_id is None or not str(tenant_id):
        raise ValueError(f"{table} pk has no tenant_id: {pk!r}")
    try:
        return str(UUID(str(tenant_id)))
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError(f"{table} pk has invalid tenant_id: {tenant_id!r}") from exc


def _entity_id(tenant_id: str, mmsi: int | str) -> str:
    return f"{tenant_id}:{int(mmsi)}"


def _split_sanctions_id(flag_id: str) -> tuple[str, str]:
    """The 'mmsi:regime' primary key -> (entity_id, regime)."""
    mmsi, sep, regime = str(flag_id).partition(":")
    if not sep or not mmsi or not regime:
        raise ValueError(f"sanctions_flags id is not 'mmsi:regime': {flag_id!r}")
    return mmsi, regime


def key_for(table: str, pk: dict[str, Any]) -> dict[str, dict[str, str]]:
    """The DynamoDB key for one source-row identity. Unknown tables fail loud:
    a new captured table must be mapped here deliberately."""
    if table in TABLE_FEATURES:
        tenant_id = _tenant_id(pk, table)
        mmsi = pk.get("mmsi")
        if mmsi is None:
            raise ValueError(f"{table} pk has no mmsi: {pk!r}")
        return {
            "entity_id": {"S": _entity_id(tenant_id, mmsi)},
            "feature_name": {"S": TABLE_FEATURES[table]},
        }
    if table == SANCTIONS_TABLE:
        tenant_id = _tenant_id(pk, table)
        flag_id = pk.get("id")
        if flag_id is None:
            raise ValueError(f"{table} pk has no id: {pk!r}")
        mmsi, regime = _split_sanctions_id(flag_id)
        return {
            "entity_id": {"S": _entity_id(tenant_id, mmsi)},
            "feature_name": {"S": FEATURE_SANCTIONS_PREFIX + regime},
        }
    raise ValueError(f"no online mapping for table {table!r}; map it in cdc/sinks/dynamo.py")


def _typed(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return {"BOOL": value}
    if isinstance(value, int | float):
        return {"N": str(value)}
    if isinstance(value, str):
        return {"S": value}
    return {"S": json.dumps(value, sort_keys=True)}


def item_for_upsert(
    table: str, pk: dict[str, Any], row: dict[str, Any], lsn: int
) -> dict[str, Any]:
    """The whole item an applied upsert PUTs. Pure; golden-tested."""
    item: dict[str, Any] = dict(key_for(table, pk))
    item["last_applied_lsn"] = {"N": str(int(lsn))}
    item["deleted"] = {"BOOL": False}
    for k, v in row.items():
        if k in ("entity_id", "feature_name", "last_applied_lsn", "deleted"):
            continue  # never let payload columns shadow the key/guard attributes
        tv = _typed(v)
        if tv is not None:
            item[k] = tv
    return item


def item_for_soft_delete(table: str, pk: dict[str, Any], lsn: int) -> dict[str, Any]:
    """The canonical delete marker: key + guard + deleted, row content dropped."""
    item: dict[str, Any] = dict(key_for(table, pk))
    item["last_applied_lsn"] = {"N": str(int(lsn))}
    item["deleted"] = {"BOOL": True}
    return item


def _is_conditional_check_failure(exc: Exception) -> bool:
    code = ""
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = str(response.get("Error", {}).get("Code", ""))
    return "ConditionalCheckFailed" in code or "ConditionalCheckFailed" in type(exc).__name__


class OnlineStoreSink:
    """StateSink over DynamoDB. The boto3 client is injected (endpoint_url from
    config points it at DynamoDB Local on the kind stack).

    guard=False strips the ConditionExpression: DRILL-ONLY (war story P2 shows
    what at-least-once delivery does to an unguarded sink). The consumer
    refuses to build it unless HM_DRILL=1 (cdc/consumer/service.py)."""

    def __init__(self, *, client: Any, table_name: str, guard: bool = True) -> None:
        self._client = client
        self._table = table_name
        self._guard = guard

    def _put(self, item: dict[str, Any], lsn: int) -> bool:
        kwargs: dict[str, Any] = {"TableName": self._table, "Item": item}
        if self._guard:
            kwargs["ConditionExpression"] = CONDITION_EXPRESSION
            kwargs["ExpressionAttributeValues"] = {":lsn": {"N": str(int(lsn))}}
        try:
            self._client.put_item(**kwargs)
        except Exception as exc:
            if _is_conditional_check_failure(exc):
                return False  # stale or duplicate delivery; the guard held
            raise
        return True

    def upsert(self, table: str, pk: dict[str, Any], row: dict[str, Any], lsn: int) -> bool:
        return self._put(item_for_upsert(table, pk, row, lsn), lsn)

    def soft_delete(self, table: str, pk: dict[str, Any], lsn: int) -> bool:
        return self._put(item_for_soft_delete(table, pk, lsn), lsn)

    def flush(self) -> None:
        # put_item is synchronous and unbuffered; nothing to flush. The method
        # exists so the applier's flush-then-commit barrier is uniform.
        return None
