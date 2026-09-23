"""Registry store + /v1/registry/* endpoint tests (Phase 2, gate C1).

The memory backend mirrors the Postgres row schema; a live-Postgres integration
test (DDL idempotence + per-table round-trip) runs only when HM_TEST_PG_DSN is set.
"""

from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.errors import RegistryEntryNotFound
from app.main import app
from app.registry import MemoryRegistryBackend, PostgresRegistryBackend, RegistryStore

MMSI = 200000003


# ------------------------------------------------------------- memory backend


async def test_vessel_upsert_and_get_round_trip():
    store = RegistryStore(MemoryRegistryBackend())
    await store.upsert_vessel(MMSI, {"name": "HM DEMO VESSEL B", "flag_state": "PA"})
    row = await store.get_vessel(MMSI)
    assert row["name"] == "HM DEMO VESSEL B"
    assert row["flag_state"] == "PA"
    # upsert overwrites
    await store.upsert_vessel(MMSI, {"name": "HM DEMO VESSEL B", "flag_state": "SG"})
    assert (await store.get_vessel(MMSI))["flag_state"] == "SG"


async def test_get_unknown_vessel_raises_not_found():
    store = RegistryStore(MemoryRegistryBackend())
    with pytest.raises(RegistryEntryNotFound):
        await store.get_vessel(MMSI)


async def test_watchlist_upsert_list_delete():
    store = RegistryStore(MemoryRegistryBackend())
    await store.upsert_watchlist(MMSI, {"reason": "dark rendezvous", "severity": 0.8})
    await store.upsert_watchlist(MMSI + 1, {"reason": "spoofing suspicion"})
    rows = await store.list_watchlist()
    assert [r["mmsi"] for r in rows] == [MMSI, MMSI + 1]
    assert rows[0]["severity"] == 0.8
    await store.delete_watchlist(MMSI)
    assert [r["mmsi"] for r in await store.list_watchlist()] == [MMSI + 1]


async def test_watchlist_upsert_keeps_created_at_and_bumps_updated_at():
    store = RegistryStore(MemoryRegistryBackend())
    first = await store.upsert_watchlist(MMSI, {"reason": "a"})
    second = await store.upsert_watchlist(MMSI, {"reason": "b"})
    assert second["created_at"] == first["created_at"]
    assert second["updated_at"] >= first["updated_at"]
    assert second["reason"] == "b"


async def test_delete_absent_watchlist_raises_not_found():
    store = RegistryStore(MemoryRegistryBackend())
    with pytest.raises(RegistryEntryNotFound):
        await store.delete_watchlist(MMSI)


async def test_sanctions_upsert_is_keyed_by_mmsi_and_regime():
    store = RegistryStore(MemoryRegistryBackend())
    await store.upsert_sanction(MMSI, {"regime": "OFAC"})
    await store.upsert_sanction(MMSI, {"regime": "OFAC", "reference": "SDN-EXAMPLE-0001"})  # upsert
    await store.upsert_sanction(MMSI, {"regime": "EU"})
    assert await store.delete_sanctions(MMSI) == 2


async def test_delete_sanctions_with_none_raises_not_found():
    store = RegistryStore(MemoryRegistryBackend())
    with pytest.raises(RegistryEntryNotFound):
        await store.delete_sanctions(MMSI)


async def test_configured_registry_propagates_missing_runtime_dependency(monkeypatch):
    async def missing_dependency(*_args, **_kwargs):
        raise ModuleNotFoundError("No module named 'asyncpg'", name="asyncpg")

    monkeypatch.setattr(PostgresRegistryBackend, "connect", missing_dependency)
    with pytest.raises(ModuleNotFoundError, match="asyncpg"):
        await RegistryStore.connect(Settings(pg_dsn="postgresql://configured"))


async def test_configured_registry_propagates_schema_failure(monkeypatch):
    async def broken_schema(*_args, **_kwargs):
        raise RuntimeError("P39 schema contract failed")

    monkeypatch.setattr(PostgresRegistryBackend, "connect", broken_schema)
    with pytest.raises(RuntimeError, match="P39 schema contract failed"):
        await RegistryStore.connect(Settings(pg_dsn="postgresql://configured"))


async def test_configured_registry_falls_back_on_connection_failure(monkeypatch):
    async def unavailable(*_args, **_kwargs):
        raise ConnectionRefusedError("connection refused")

    monkeypatch.setattr(PostgresRegistryBackend, "connect", unavailable)
    store = await RegistryStore.connect(Settings(pg_dsn="postgresql://configured"))
    assert isinstance(store.backend, MemoryRegistryBackend)


@pytest.mark.parametrize(
    "error",
    (FileNotFoundError("missing TLS certificate"), PermissionError("TLS key is unreadable")),
)
async def test_configured_registry_propagates_non_network_os_error(monkeypatch, error):
    async def misconfigured(*_args, **_kwargs):
        raise error

    monkeypatch.setattr(PostgresRegistryBackend, "connect", misconfigured)
    with pytest.raises(type(error), match=str(error)):
        await RegistryStore.connect(Settings(pg_dsn="postgresql://configured"))


# ------------------------------------------------------------------ endpoints


def test_registry_endpoints_crud_cycle():
    with TestClient(app) as c:
        # vessel
        assert c.get(f"/v1/registry/vessels/{MMSI}").status_code == 404
        r = c.put(f"/v1/registry/vessels/{MMSI}", json={"name": "HM DEMO VESSEL B"})
        assert r.status_code == 200 and r.json()["name"] == "HM DEMO VESSEL B"
        assert c.get(f"/v1/registry/vessels/{MMSI}").json()["mmsi"] == MMSI

        # watchlist
        assert c.get("/v1/registry/watchlist").json() == []
        r = c.put(f"/v1/registry/watchlist/{MMSI}", json={"reason": "dark rendezvous"})
        assert r.status_code == 200 and r.json()["severity"] == 0.9
        assert [x["mmsi"] for x in c.get("/v1/registry/watchlist").json()] == [MMSI]
        assert c.delete(f"/v1/registry/watchlist/{MMSI}").json()["deleted"] is True
        assert c.get("/v1/registry/watchlist").json() == []
        assert c.delete(f"/v1/registry/watchlist/{MMSI}").status_code == 404

        # sanctions
        assert c.put(f"/v1/registry/sanctions/{MMSI}", json={"regime": "OFAC"}).status_code == 200
        assert c.delete(f"/v1/registry/sanctions/{MMSI}").json()["deleted"] == 1
        assert c.delete(f"/v1/registry/sanctions/{MMSI}").status_code == 404


def test_registry_endpoint_input_validation():
    with TestClient(app) as c:
        # empty reason and out-of-range severity are rejected by the model
        assert c.put(f"/v1/registry/watchlist/{MMSI}", json={"reason": ""}).status_code == 422
        assert (
            c.put(
                f"/v1/registry/watchlist/{MMSI}", json={"reason": "x", "severity": 1.5}
            ).status_code
            == 422
        )
        # non-MMSI path values are rejected by the Path constraint
        assert c.put("/v1/registry/watchlist/9999999991", json={"reason": "x"}).status_code == 422
        # blank-after-strip values are rejected: they would mint poison CDC ids
        assert c.put(f"/v1/registry/sanctions/{MMSI}", json={"regime": "  "}).status_code == 422
        assert c.put(f"/v1/registry/watchlist/{MMSI}", json={"reason": "  "}).status_code == 422
        # unbounded strings are rejected: a DynamoDB item caps at 400 KB
        assert (
            c.put(f"/v1/registry/watchlist/{MMSI}", json={"reason": "x" * 5000}).status_code == 422
        )


# ---------------------------------------------------------- postgres (opt-in)


@pytest.mark.postgres
@pytest.mark.skipif(not os.getenv("HM_TEST_PG_DSN"), reason="set HM_TEST_PG_DSN to run")
async def test_postgres_backend_ddl_idempotent_and_round_trips():
    from cdc.schema import ddl

    backend = await PostgresRegistryBackend.connect(os.environ["HM_TEST_PG_DSN"])
    try:
        # connect() already ran the DDL once; run it again to prove idempotence
        async with backend._pool.acquire() as conn:
            for stmt in ddl.statements():
                await conn.execute(stmt)

        await backend.upsert_vessel(MMSI, {"name": "HM DEMO VESSEL B"})
        assert (await backend.get_vessel(MMSI))["name"] == "HM DEMO VESSEL B"
        await backend.upsert_watchlist(MMSI, {"reason": "integration"})
        assert [r["mmsi"] for r in await backend.list_watchlist() if r["mmsi"] == MMSI]
        await backend.upsert_sanction(MMSI, {"regime": "OFAC"})
        assert await backend.delete_sanctions(MMSI) == 1
        assert await backend.delete_watchlist(MMSI) is True
        async with backend._pool.acquire() as conn:
            await conn.execute("DELETE FROM vessels WHERE mmsi = $1", MMSI)
    finally:
        await backend.close()
