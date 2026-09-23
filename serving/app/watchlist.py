"""CDC-fed online watchlist read path (Phase 2, gate C2).

The scorer never reads Postgres. It reads the online store the CDC consumer
maintains: one DynamoDB Query per vessel (entity_id = tenant_id:mmsi) returns the
vessel_meta item, the watchlist item, and every sanctions:<regime> item in a
single round trip, with Redis as a read-through cache in front. The CDC
consumer's Redis sink DELs the same key on any applied change, so the cache is
invalidated within the pipeline's freshness budget; the TTL is only a staleness
backstop if an invalidation is lost.

Failure policy is FAIL-OPEN, deliberately: if both Redis and DynamoDB are
unreachable the event still scores, without the watchlist reason, and
WATCHLIST_LOOKUP_ERRORS increments. Availability over freshness, stated and
metered (docs/phases/PHASE_2.md, decisions). The inverse would let a cache
outage stop all scoring.

Online item layout (must match cdc/sinks/dynamo.py; drift-guarded by a test):
  entity_id     S  tenant_id:mmsi
  feature_name  S  "vessel_meta" | "watchlist" | "sanctions:<regime>"
  deleted       BOOL  soft-delete marker (read as absent)
  last_applied_lsn  N  the idempotency guard, owned by the CDC sink
  ... payload attributes from the source row (reason, severity, regime, ...)
"""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, field
from typing import Any, Protocol
from uuid import UUID

import structlog

from app.config import DEFAULT_TENANT_ID, Settings
from app.metrics import WATCHLIST_LOOKUP_ERRORS

log = structlog.get_logger(__name__)

# Key vocabulary shared with the CDC consumer's sinks. The serving path keeps
# local copies so it does not import consumer-only sink modules; a unit test at
# gate C5 asserts they are identical, so they cannot drift silently.
FEATURE_VESSEL_META = "vessel_meta"
FEATURE_WATCHLIST = "watchlist"
FEATURE_SANCTIONS_PREFIX = "sanctions:"


def online_entity_id(mmsi: int, tenant_id: str = DEFAULT_TENANT_ID) -> str:
    """Tenant-qualified DynamoDB partition key for one vessel."""
    if not tenant_id:
        raise ValueError("tenant_id must not be empty")
    return f"{UUID(tenant_id)}:{int(mmsi)}"


def redis_key(mmsi: int, tenant_id: str = DEFAULT_TENANT_ID) -> str:
    """The cache key for one vessel's online status. The CDC Redis sink DELs it."""
    return f"hm:online:{online_entity_id(mmsi, tenant_id)}"


@dataclass(frozen=True)
class WatchlistStatus:
    """Parsed online state for one vessel."""

    watchlisted: bool = False
    reason: str = ""
    severity: float = 0.9
    sanctions: tuple[str, ...] = ()
    vessel: dict[str, Any] = field(default_factory=dict)

    @property
    def sanctioned(self) -> bool:
        return bool(self.sanctions)

    def to_json(self) -> str:
        return json.dumps(
            {
                "watchlisted": self.watchlisted,
                "reason": self.reason,
                "severity": self.severity,
                "sanctions": list(self.sanctions),
                "vessel": self.vessel,
            }
        )

    @classmethod
    def from_json(cls, raw: str | bytes) -> WatchlistStatus:
        d = json.loads(raw)
        return cls(
            watchlisted=bool(d.get("watchlisted", False)),
            reason=str(d.get("reason", "")),
            severity=float(d.get("severity", 0.9)),
            sanctions=tuple(d.get("sanctions", ())),
            vessel=dict(d.get("vessel", {})),
        )


EMPTY_STATUS = WatchlistStatus()


class DynamoQueryClient(Protocol):
    def query(self, **kwargs: Any) -> dict[str, Any]: ...


class RedisClient(Protocol):
    def get(self, key: str) -> bytes | None: ...
    def setex(self, key: str, ttl_s: int, value: str) -> Any: ...


def _attr(item: dict[str, Any], name: str, default: Any = None) -> Any:
    """Unwrap one DynamoDB typed attribute ({'S': ...} / {'N': ...} / {'BOOL': ...})."""
    v = item.get(name)
    if not isinstance(v, dict):
        return default
    if "S" in v:
        return v["S"]
    if "N" in v:
        return float(v["N"])
    if "BOOL" in v:
        return bool(v["BOOL"])
    return default


def parse_online_items(items: list[dict[str, Any]]) -> WatchlistStatus:
    """Fold one vessel's online items into a WatchlistStatus. Pure."""
    watchlisted = False
    reason = ""
    severity = 0.9
    sanctions: list[str] = []
    vessel: dict[str, Any] = {}
    for item in items:
        if _attr(item, "deleted", False):
            continue  # soft-delete marker: read as absent
        feature = _attr(item, "feature_name", "")
        if feature == FEATURE_WATCHLIST:
            watchlisted = True
            reason = str(_attr(item, "reason", "") or "")
            severity_attr = item.get("severity")
            if isinstance(severity_attr, dict) and "N" in severity_attr:
                severity = float(severity_attr["N"])
            else:
                severity = float(_attr(item, "severity", 0.9) or 0.9)
        elif feature == FEATURE_VESSEL_META:
            vessel = {
                k: _attr(item, k, "")
                for k in ("name", "flag_state", "vessel_type")
                if _attr(item, k) is not None
            }
        elif isinstance(feature, str) and feature.startswith(FEATURE_SANCTIONS_PREFIX):
            sanctions.append(feature.removeprefix(FEATURE_SANCTIONS_PREFIX))
    return WatchlistStatus(
        watchlisted=watchlisted,
        reason=reason,
        severity=severity,
        sanctions=tuple(sorted(sanctions)),
        vessel=vessel,
    )


class WatchlistLookup:
    """Read-through lookup: Redis -> DynamoDB Query -> populate Redis (TTL backstop).

    Both clients are injected; either may be None (no cache layer / lookup
    disabled). Every failure path is fail-open and counted.
    """

    def __init__(
        self,
        *,
        ddb_client: DynamoQueryClient | None,
        redis_client: RedisClient | None,
        table: str,
        cache_ttl_s: int = 300,
        tenant_id: str = DEFAULT_TENANT_ID,
    ) -> None:
        self._ddb = ddb_client
        self._redis = redis_client
        self._table = table
        self._ttl = cache_ttl_s
        self._tenant_id = tenant_id
        # Single-flight coalescing: concurrent cache-miss reads for the same
        # vessel share one in-flight backing fetch instead of each spawning a
        # worker thread that hits DynamoDB. Keyed by mmsi; entries are removed
        # once the fetch resolves. The lock only guards the small map mutation,
        # not the fetch itself, so distinct vessels never serialize.
        self._inflight: dict[str, asyncio.Future[WatchlistStatus]] = {}
        # Bound to the running loop on first use: __init__ may run off any loop
        # (e.g. from_settings at bootstrap), and an asyncio.Lock binds to the
        # loop that first awaits it.
        self._inflight_lock: asyncio.Lock | None = None

    @property
    def enabled(self) -> bool:
        return self._ddb is not None and bool(self._table)

    @classmethod
    def from_settings(cls, settings: Settings) -> WatchlistLookup:
        """Build real clients from config; degrade to disabled when unset/missing.

        Both clients get tight timeouts: this lookup runs on the scoring path,
        so a partitioned backend must raise (and fail open) in ~1 s, never hang
        the scorer. Region always resolves (env fallback us-east-1) and the
        DynamoDB Local endpoint gets placeholder credentials, matching the CDC
        consumer's client construction.
        """
        ddb = None
        redis_client = None
        if settings.online_table:
            try:
                import boto3  # lazy optional dep (the [serving-runtime] extra)
                from botocore.config import Config as BotoConfig

                kwargs: dict[str, Any] = {
                    "region_name": os.environ.get(
                        "AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
                    ),
                    "config": BotoConfig(
                        connect_timeout=1,
                        read_timeout=1,
                        retries={"max_attempts": 2, "mode": "standard"},
                    ),
                }
                if settings.ddb_endpoint_url:
                    kwargs["endpoint_url"] = settings.ddb_endpoint_url
                    kwargs["aws_access_key_id"] = "local"
                    kwargs["aws_secret_access_key"] = "local"  # nosec B105  # dummy creds for local DynamoDB endpoint only, gated on ddb_endpoint_url
                ddb = boto3.client("dynamodb", **kwargs)
            except Exception as exc:
                log.warning("watchlist_ddb_unavailable_lookup_disabled", err=str(exc))
        if settings.redis_url:
            try:
                import redis  # lazy optional dep (the [serving-runtime] extra)

                redis_client = redis.Redis.from_url(
                    settings.redis_url, socket_connect_timeout=0.5, socket_timeout=0.5
                )
            except Exception as exc:
                log.warning("watchlist_redis_unavailable_cache_disabled", err=str(exc))
        return cls(
            ddb_client=ddb,
            redis_client=redis_client,
            table=settings.online_table,
            cache_ttl_s=settings.watchlist_cache_ttl_s,
            tenant_id=settings.resolved_tenant_id(),
        )

    async def aget(self, mmsi: int) -> WatchlistStatus:
        """Event-loop-safe, single-flight lookup.

        The blocking redis/boto3 calls run in a worker thread so one slow
        backend cannot stall every request on the loop. Concurrent cache-miss
        reads for the SAME vessel are coalesced: the first caller starts one
        backing fetch and every other caller awaits its result, so a hot key
        makes exactly one DynamoDB round trip instead of one per request. The
        in-flight entry is removed once the fetch resolves, in success and
        failure alike, so a transient outage cannot pin a stale future. Distinct
        vessels never serialize (the lock only guards the small map). Zero-cost
        when disabled."""
        if not self.enabled:
            return EMPTY_STATUS

        if self._inflight_lock is None:
            self._inflight_lock = asyncio.Lock()

        key = online_entity_id(mmsi, self._tenant_id)
        async with self._inflight_lock:
            existing = self._inflight.get(key)
            if existing is not None:
                fut = existing
            else:
                fut = asyncio.ensure_future(asyncio.to_thread(self.get, mmsi))
                self._inflight[key] = fut
                fut.add_done_callback(lambda _f, k=key: self._inflight.pop(k, None))

        # Await outside the lock so distinct vessels (and the fetch itself) run
        # concurrently. asyncio.shield keeps a caller's cancellation from
        # cancelling the shared fetch out from under the other coalesced waiters.
        return await asyncio.shield(fut)

    def get(self, mmsi: int) -> WatchlistStatus:
        if not self.enabled:
            return EMPTY_STATUS

        key = redis_key(mmsi, self._tenant_id)
        if self._redis is not None:
            try:
                cached = self._redis.get(key)
                if cached is not None:
                    return WatchlistStatus.from_json(cached)
            except Exception as exc:  # cache outage never blocks scoring
                WATCHLIST_LOOKUP_ERRORS.inc()
                log.warning("watchlist_cache_read_failed", mmsi=mmsi, err=str(exc))

        try:
            assert self._ddb is not None  # nosec B101  # internal invariant, not validation of untrusted input; guarded by self.enabled
            resp = self._ddb.query(
                TableName=self._table,
                KeyConditionExpression="entity_id = :e",
                ExpressionAttributeValues={":e": {"S": online_entity_id(mmsi, self._tenant_id)}},
                ConsistentRead=False,
            )
            status = parse_online_items(resp.get("Items", []))
        except Exception as exc:  # fail-open: score without the watchlist reason
            WATCHLIST_LOOKUP_ERRORS.inc()
            log.warning("watchlist_lookup_failed_fail_open", mmsi=mmsi, err=str(exc))
            return EMPTY_STATUS

        if self._redis is not None:
            try:
                self._redis.setex(key, self._ttl, status.to_json())
            except Exception as exc:  # cache write failure is not a lookup failure
                WATCHLIST_LOOKUP_ERRORS.inc()
                log.warning("watchlist_cache_write_failed", mmsi=mmsi, err=str(exc))
        return status
