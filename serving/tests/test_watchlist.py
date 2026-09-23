"""Online watchlist read-path tests (Phase 2, gate C2).

Covers the pure item parser, the read-through cache order, fail-open behavior,
and the scoring fusion: a watchlisted vessel gains WATCHLIST_HIT and lands in
HITL; an unseeded vessel scores exactly as in Phase 1 (goldens unchanged).
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any

import pytest

from app.config import Settings
from app.metrics import WATCHLIST_LOOKUP_ERRORS
from app.watchlist import (
    EMPTY_STATUS,
    FEATURE_SANCTIONS_PREFIX,
    FEATURE_VESSEL_META,
    FEATURE_WATCHLIST,
    WatchlistLookup,
    WatchlistStatus,
    online_entity_id,
    parse_online_items,
    redis_key,
)

from ._helpers import build_score_in

MMSI = 200000003
TENANT_A = "11111111-1111-1111-1111-111111111111"
TENANT_B = "22222222-2222-2222-2222-222222222222"


def _item(feature: str, *, tenant_id: str | None = None, **attrs: Any) -> dict:
    out: dict[str, Any] = {
        "entity_id": {
            "S": online_entity_id(MMSI, tenant_id) if tenant_id else online_entity_id(MMSI)
        },
        "feature_name": {"S": feature},
    }
    for k, v in attrs.items():
        if isinstance(v, bool):
            out[k] = {"BOOL": v}
        elif isinstance(v, int | float):
            out[k] = {"N": str(v)}
        else:
            out[k] = {"S": str(v)}
    return out


class FakeDdb:
    def __init__(self, items: list[dict] | None = None, err: Exception | None = None) -> None:
        self.items = items or []
        self.err = err
        self.calls: list[dict] = []

    def query(self, **kwargs: Any) -> dict:
        self.calls.append(kwargs)
        if self.err:
            raise self.err
        return {"Items": self.items}


class PartitionedFakeDdb(FakeDdb):
    """Filter the shared fake table by the requested DynamoDB partition."""

    def query(self, **kwargs: Any) -> dict:
        self.calls.append(kwargs)
        requested = kwargs["ExpressionAttributeValues"][":e"]["S"]
        return {
            "Items": [
                item for item in self.items if item.get("entity_id", {}).get("S") == requested
            ]
        }


class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.setex_calls: list[tuple[str, int, str]] = []

    def get(self, key: str) -> bytes | None:
        v = self.store.get(key)
        return v.encode() if v is not None else None

    def setex(self, key: str, ttl_s: int, value: str) -> None:
        self.setex_calls.append((key, ttl_s, value))
        self.store[key] = value


# ------------------------------------------------------------------ parsing


def test_parse_online_items_watchlist_and_sanctions_and_meta():
    status = parse_online_items(
        [
            _item(FEATURE_WATCHLIST, reason="dark rendezvous", severity=0.8),
            _item(FEATURE_SANCTIONS_PREFIX + "ofac", regime="ofac"),
            _item(FEATURE_SANCTIONS_PREFIX + "eu", regime="eu"),
            _item(FEATURE_VESSEL_META, name="HM DEMO VESSEL B", flag_state="PA"),
        ]
    )
    assert status.watchlisted is True
    assert status.reason == "dark rendezvous"
    assert status.severity == 0.8
    assert status.sanctions == ("eu", "ofac")
    assert status.vessel["name"] == "HM DEMO VESSEL B"


def test_parse_online_items_preserves_explicit_zero_severity():
    status = parse_online_items([_item(FEATURE_WATCHLIST, severity=0.0)])
    assert status.severity == 0.0


def test_parse_online_items_defaults_missing_severity():
    status = parse_online_items([_item(FEATURE_WATCHLIST)])
    assert status.severity == 0.9


@pytest.mark.parametrize("raw_severity", [{"BOOL": False}, {"S": ""}])
def test_parse_online_items_keeps_falsey_nonnumeric_fallback(raw_severity):
    item = _item(FEATURE_WATCHLIST)
    item["severity"] = raw_severity
    assert parse_online_items([item]).severity == 0.9


def test_parse_online_items_soft_delete_marker_reads_as_absent():
    status = parse_online_items(
        [_item(FEATURE_WATCHLIST, reason="stale", deleted=True, last_applied_lsn=42)]
    )
    assert status == EMPTY_STATUS


def test_status_json_round_trip():
    s = WatchlistStatus(watchlisted=True, reason="r", severity=0.7, sanctions=("ofac",))
    assert WatchlistStatus.from_json(s.to_json()) == s


# ------------------------------------------------------------------- lookup


def test_disabled_lookup_returns_empty_without_calls():
    lookup = WatchlistLookup(ddb_client=None, redis_client=None, table="")
    assert lookup.enabled is False
    assert lookup.get(MMSI) == EMPTY_STATUS


def test_cache_miss_queries_ddb_and_populates_with_ttl():
    ddb = FakeDdb([_item(FEATURE_WATCHLIST, reason="x")])
    r = FakeRedis()
    lookup = WatchlistLookup(ddb_client=ddb, redis_client=r, table="t", cache_ttl_s=123)
    status = lookup.get(MMSI)
    assert status.watchlisted is True
    assert len(ddb.calls) == 1
    assert ddb.calls[0]["TableName"] == "t"
    (key, ttl, value) = r.setex_calls[0]
    assert key == redis_key(MMSI) and ttl == 123
    assert WatchlistStatus.from_json(value) == status


def test_cache_hit_skips_ddb():
    ddb = FakeDdb([_item(FEATURE_WATCHLIST, reason="x")])
    r = FakeRedis()
    lookup = WatchlistLookup(ddb_client=ddb, redis_client=r, table="t")
    lookup.get(MMSI)  # miss populates
    lookup.get(MMSI)  # hit
    assert len(ddb.calls) == 1


def test_settings_tenant_reaches_the_lookup():
    lookup = WatchlistLookup.from_settings(Settings(tenant_id=TENANT_A))
    assert lookup._tenant_id == TENANT_A


@pytest.mark.parametrize("tenant_id", ["", None, "not-a-uuid"])
def test_online_keys_reject_invalid_tenant_ids(tenant_id):
    with pytest.raises((TypeError, ValueError)):
        online_entity_id(MMSI, tenant_id)


def test_same_mmsi_cannot_share_cache_or_ddb_partition_across_tenants():
    redis = FakeRedis()
    ddb = PartitionedFakeDdb(
        [
            _item(FEATURE_WATCHLIST, tenant_id=TENANT_B, reason="tenant B"),
            _item(FEATURE_WATCHLIST, tenant_id=TENANT_A, reason="tenant A"),
        ]
    )
    lookup_a = WatchlistLookup(
        ddb_client=ddb,
        redis_client=redis,
        table="t",
        tenant_id=TENANT_A,
    )
    lookup_b = WatchlistLookup(
        ddb_client=ddb,
        redis_client=redis,
        table="t",
        tenant_id=TENANT_B,
    )
    assert lookup_a.get(MMSI).reason == "tenant A"
    assert lookup_b.get(MMSI).reason == "tenant B"
    assert ddb.calls[0]["ExpressionAttributeValues"] == {
        ":e": {"S": online_entity_id(MMSI, TENANT_A)}
    }
    assert ddb.calls[1]["ExpressionAttributeValues"] == {
        ":e": {"S": online_entity_id(MMSI, TENANT_B)}
    }
    assert redis_key(MMSI, TENANT_A) in redis.store
    assert redis_key(MMSI, TENANT_B) in redis.store
    assert WatchlistStatus.from_json(redis.store[redis_key(MMSI, TENANT_A)]).reason == "tenant A"
    assert WatchlistStatus.from_json(redis.store[redis_key(MMSI, TENANT_B)]).reason == "tenant B"

    # Subsequent reads stay isolated in their tenant-specific cache entries.
    assert lookup_a.get(MMSI).reason == "tenant A"
    assert lookup_b.get(MMSI).reason == "tenant B"
    assert len(ddb.calls) == 2


def test_ddb_error_fails_open_and_counts():
    before = WATCHLIST_LOOKUP_ERRORS._value.get()
    lookup = WatchlistLookup(
        ddb_client=FakeDdb(err=RuntimeError("boom")), redis_client=None, table="t"
    )
    assert lookup.get(MMSI) == EMPTY_STATUS
    assert WATCHLIST_LOOKUP_ERRORS._value.get() == before + 1


# ------------------------------------------------------- single-flight (async)


async def test_aget_coalesces_concurrent_misses_into_one_fetch(monkeypatch):
    # N concurrent aget() calls for the SAME uncached vessel must share a single
    # backing fetch. A gate (threading.Event) holds the fetch in-flight until all
    # callers have coalesced onto it, so the assertion is deterministic, not a
    # timing race. The fetch runs in a worker thread (asyncio.to_thread), so the
    # gate is a threading primitive, not an asyncio one.
    ddb = FakeDdb([_item(FEATURE_WATCHLIST, reason="dark rendezvous", severity=0.8)])
    lookup = WatchlistLookup(ddb_client=ddb, redis_client=None, table="t")

    release = threading.Event()
    calls = 0

    real_get = lookup.get

    def slow_get(mmsi: int) -> WatchlistStatus:
        nonlocal calls
        calls += 1
        # block the in-flight fetch so late callers must coalesce, not restart it
        assert release.wait(timeout=5.0), "fetch gate never released"
        return real_get(mmsi)

    monkeypatch.setattr(lookup, "get", slow_get)

    n = 12
    tasks = [asyncio.ensure_future(lookup.aget(MMSI)) for _ in range(n)]

    # let every caller run its lock section and coalesce onto the one future
    for _ in range(500):
        await asyncio.sleep(0)
        if online_entity_id(MMSI) in lookup._inflight and sum(t.done() for t in tasks) == 0:
            break
    release.set()
    results = await asyncio.gather(*tasks)

    assert calls == 1  # coalesced: exactly one backing fetch for all N callers
    assert len(ddb.calls) == 1  # and exactly one DynamoDB round trip
    assert all(r == results[0] for r in results)
    assert all(r.watchlisted is True and r.reason == "dark rendezvous" for r in results)
    # the in-flight entry is cleaned up after resolution
    assert lookup._inflight == {}


async def test_aget_after_coalesced_flight_can_fetch_again(monkeypatch):
    # once the in-flight entry is cleared, a later miss starts a fresh fetch
    # (the coalescing map must not pin the first result forever).
    ddb = FakeDdb([_item(FEATURE_WATCHLIST, reason="x")])
    lookup = WatchlistLookup(ddb_client=ddb, redis_client=None, table="t")
    calls = 0
    real_get = lookup.get

    def counting_get(mmsi: int) -> WatchlistStatus:
        nonlocal calls
        calls += 1
        return real_get(mmsi)

    monkeypatch.setattr(lookup, "get", counting_get)

    await asyncio.gather(*[lookup.aget(MMSI) for _ in range(4)])
    assert calls == 1 and lookup._inflight == {}
    await lookup.aget(MMSI)  # separate, non-overlapping flight
    assert calls == 2


# ------------------------------------------------------------------- fusion


async def test_watchlisted_vessel_gains_hit_reason_and_hitl(orch, fixture_by_mmsi, expectations):
    ns = expectations["normal_samples"][0]
    ddb = FakeDdb([_item(FEATURE_WATCHLIST, reason="dark rendezvous", severity=0.8)])
    orch.watchlist = WatchlistLookup(ddb_client=ddb, redis_client=None, table="t")
    # the fake store answers for every mmsi; the normal event now scores as a hit
    out = await orch.score(build_score_in(fixture_by_mmsi, ns["mmsi"], ns["t"]))
    hits = [r for r in out.reasons if r.code.value == "watchlist_hit"]
    assert len(hits) == 1
    assert hits[0].severity == orch.settings.watchlist_severity == 0.9
    assert out.hitl_required is True
    assert out.score >= orch.settings.anomaly_hitl_threshold


async def test_sanctioned_vessel_gains_sanctions_reason(orch, fixture_by_mmsi, expectations):
    ns = expectations["normal_samples"][0]
    ddb = FakeDdb([_item(FEATURE_SANCTIONS_PREFIX + "ofac", regime="ofac")])
    orch.watchlist = WatchlistLookup(ddb_client=ddb, redis_client=None, table="t")
    out = await orch.score(build_score_in(fixture_by_mmsi, ns["mmsi"], ns["t"]))
    hits = [r for r in out.reasons if r.code.value == "sanctions_hit"]
    assert len(hits) == 1
    assert hits[0].severity == orch.settings.sanctions_severity == 0.95
    assert out.hitl_required is True


async def test_unseeded_vessel_scores_exactly_as_phase1(orch, fixture_by_mmsi, expectations):
    # goldens unchanged: the default orchestrator has the lookup disabled
    assert orch.watchlist.enabled is False
    for ns in expectations["normal_samples"]:
        out = await orch.score(build_score_in(fixture_by_mmsi, ns["mmsi"], ns["t"]))
        assert out.reasons == [] and out.hitl_required is False


async def test_lookup_failure_never_blocks_scoring(orch, fixture_by_mmsi, expectations):
    ns = expectations["normal_samples"][0]
    orch.watchlist = WatchlistLookup(
        ddb_client=FakeDdb(err=RuntimeError("outage")), redis_client=None, table="t"
    )
    out = await orch.score(build_score_in(fixture_by_mmsi, ns["mmsi"], ns["t"]))
    assert out.reasons == [] and out.hitl_required is False
