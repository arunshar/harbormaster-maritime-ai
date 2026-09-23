"""Redis invalidation sink (Phase 2, gate C5).

Write-invalidate, not write-through: on every DELIVERED change event the
consumer DELs the vessel's cache key and the scorer's next read-through
(serving/app/watchlist.py) repopulates from DynamoDB. Firing on guard-rejected
redeliveries too (not just applied changes) is deliberate: DEL is idempotent,
and a rejected redelivery is exactly the signal that a prior attempt may have
died after the store write but before the invalidation.

Honest staleness bound: cache-aside has an inherent race (a reader that missed
just before the DEL can repopulate pre-change state just after it), and a DEL
against a partitioned Redis can be lost outright. Both are bounded by the
serving-side TTL (HM_WATCHLIST_CACHE_TTL_S, default 300 s); that bound, not
"never stale", is the guarantee.

The key rule is duplicated from serving/app/watchlist.py redis_key(); a test
in cdc/tests/test_sinks.py holds the two identical.
"""

from __future__ import annotations

from typing import Any

import structlog

from cdc.consumer.envelope import ChangeEvent
from cdc.sinks.dynamo import SANCTIONS_TABLE, _entity_id, _split_sanctions_id, _tenant_id

log = structlog.get_logger(__name__)

REDIS_KEY_PREFIX = "hm:online:"


def redis_key_for_event(event: ChangeEvent) -> str:
    """The tenant-qualified cache key an applied change invalidates."""
    tenant_id = _tenant_id(event.pk, event.table)
    if event.table == SANCTIONS_TABLE:
        flag_id = event.pk.get("id")
        if flag_id is None:
            raise ValueError(f"sanctions event pk has no id: {event.pk!r}")
        mmsi, _ = _split_sanctions_id(flag_id)
        return f"{REDIS_KEY_PREFIX}{_entity_id(tenant_id, mmsi)}"
    mmsi = event.pk.get("mmsi")
    if mmsi is None:
        raise ValueError(f"{event.table} event pk has no mmsi: {event.pk!r}")
    return f"{REDIS_KEY_PREFIX}{_entity_id(tenant_id, mmsi)}"


class RedisInvalidationSink:
    """EffectSink: DEL the vessel's online-status key on every delivered change."""

    def __init__(self, *, client: Any) -> None:
        self._client = client  # needs only .delete(key)

    def on_change(self, event: ChangeEvent) -> None:
        key = redis_key_for_event(event)
        self._client.delete(key)
        log.debug("cdc_cache_invalidated", key=key, table=event.table, lsn=event.lsn)

    def flush(self) -> None:
        return None
