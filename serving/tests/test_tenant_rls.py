"""Real-Postgres RLS isolation tests (Phase 5, gate 5.4).

RLS is a database-enforced guarantee a mock cannot meaningfully stand in for
(the Phase 2 convention for CDC/DB-enforced behavior), so every test here runs
against a live Postgres and the whole module is opt-in: set HM_TEST_PG_DSN to
run it (`make phase5-tenant-smoke` spins a throwaway postgres:16 container and
runs the same checks; the default suite skips hermetically).

Two Postgres facts shape the setup:
- Superusers and BYPASSRLS roles ALWAYS bypass RLS, so asserting isolation
  from the bootstrap superuser would be vacuous. Each test creates a fresh
  NOSUPERUSER role that OWNS a fresh database; FORCE ROW LEVEL SECURITY (part
  of the tenancy DDL) makes the policies bind even for that owner.
- The HM_TEST_PG_DSN role must be able to CREATE ROLE / CREATE DATABASE (the
  drill container's bootstrap user can).

The fail-closed acceptance shape (docs/phases/PHASE_5.md gate 5.4, incident
P6): a session with NO app.tenant_id set reads ZERO rows, never all rows, and
cannot write at all.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from urllib.parse import urlsplit, urlunsplit

import pytest

from app.errors import HitlTraceNotFound
from app.hitl import PostgresHitlBackend
from app.models import AisScoreOut, FeedbackIn, ReasonCode, ScoreReason
from app.registry import PostgresRegistryBackend

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(not os.getenv("HM_TEST_PG_DSN"), reason="set HM_TEST_PG_DSN to run"),
]

TS = datetime(2024, 6, 1, 3, 20, tzinfo=UTC)
TENANT_A = "11111111-1111-1111-1111-111111111111"
TENANT_B = "22222222-2222-2222-2222-222222222222"


def _out(trace_id: str) -> AisScoreOut:
    return AisScoreOut(
        mmsi=200000003,
        score=1.0,
        confidence=1.0,
        reasons=[ScoreReason(code=ReasonCode.OFF_CORRIDOR, severity=1.0, detail="10 km off")],
        hitl_required=True,
        trace_id=trace_id,
        latency_ms=0.5,
        n_history=120,
    )


def _swap_dsn(dsn: str, *, user: str, password: str, database: str) -> str:
    parts = urlsplit(dsn)
    host = parts.hostname or "127.0.0.1"
    port = f":{parts.port}" if parts.port else ""
    return urlunsplit((parts.scheme, f"{user}:{password}@{host}{port}", f"/{database}", "", ""))


@contextlib.asynccontextmanager
async def fresh_tenant_db() -> AsyncIterator[str]:
    """A throwaway database OWNED by a throwaway NOSUPERUSER role, so FORCE
    RLS binds; yields the owner DSN, then drops both."""
    import asyncpg

    admin_dsn = os.environ["HM_TEST_PG_DSN"]
    suffix = uuid.uuid4().hex[:10]
    role, db, password = f"hm_rls_owner_{suffix}", f"hm_rls_{suffix}", "hm_rls_pw"

    admin = await asyncpg.connect(dsn=admin_dsn)
    try:
        await admin.execute(
            f"CREATE ROLE {role} LOGIN PASSWORD '{password}' NOSUPERUSER NOBYPASSRLS"
        )
        await admin.execute(f"CREATE DATABASE {db} OWNER {role}")
    finally:
        await admin.close()
    try:
        yield _swap_dsn(admin_dsn, user=role, password=password, database=db)
    finally:
        admin = await asyncpg.connect(dsn=admin_dsn)
        try:
            await admin.execute(f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")
            await admin.execute(f"DROP ROLE IF EXISTS {role}")
        finally:
            await admin.close()


async def _apply_full_schema(conn) -> None:
    from app import hitl as hitl_module
    from cdc.schema import ddl, tenancy

    for stmt in ddl.statements():
        await conn.execute(stmt)
    await conn.execute(hitl_module._DDL)
    for stmt in tenancy.statements():
        await conn.execute(stmt)


async def _set_tenant(conn, tenant_id: str) -> None:
    await conn.execute("SELECT set_config('app.tenant_id', $1, false)", tenant_id)


async def _create_legacy_registry_schema(conn, *, with_tenant_id: bool = False) -> None:
    tenant_column = "tenant_id uuid NOT NULL, " if with_tenant_id else ""
    await conn.execute(
        f"CREATE TABLE vessels ({tenant_column}mmsi bigint PRIMARY KEY, "
        "name text NOT NULL DEFAULT '')"
    )
    await conn.execute(
        f"CREATE TABLE watchlist ({tenant_column}mmsi bigint PRIMARY KEY, reason text NOT NULL)"
    )
    await conn.execute(
        f"CREATE TABLE sanctions_flags ({tenant_column}id text PRIMARY KEY, "
        "mmsi bigint NOT NULL, regime text NOT NULL)"
    )


async def _grant_owner_bypass_rls(owner_dsn: str) -> None:
    import asyncpg

    admin = await asyncpg.connect(dsn=os.environ["HM_TEST_PG_DSN"])
    try:
        role = urlsplit(owner_dsn).username
        statement = await admin.fetchval("SELECT format('ALTER ROLE %I BYPASSRLS', $1::text)", role)
        await admin.execute(statement)
    finally:
        await admin.close()


async def test_each_schema_path_waits_on_the_shared_bootstrap_lock():
    """Migration and both runtime DDL paths must use one lock."""
    import asyncpg

    from cdc.schema import tenancy
    from scripts.migrate_p39 import migrate

    async with fresh_tenant_db() as owner_dsn:
        for connect in (
            lambda: migrate(owner_dsn),
            lambda: PostgresHitlBackend.connect(owner_dsn, TENANT_A),
            lambda: PostgresRegistryBackend.connect(owner_dsn, TENANT_B),
        ):
            blocker = await asyncpg.connect(dsn=owner_dsn)
            observer = await asyncpg.connect(dsn=owner_dsn)
            task = None
            try:
                async with blocker.transaction():
                    await blocker.execute(
                        "SELECT pg_advisory_xact_lock($1)",
                        tenancy.SCHEMA_BOOTSTRAP_LOCK_ID,
                    )
                    lock = await blocker.fetchrow(
                        "SELECT classid::bigint AS classid, objid::bigint AS objid, objsubid "
                        "FROM pg_locks WHERE pid = pg_backend_pid() "
                        "AND locktype = 'advisory' AND granted"
                    )
                    assert lock is not None

                    task = asyncio.create_task(connect())
                    waiting = 0
                    for _ in range(200):
                        waiting = await observer.fetchval(
                            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' "
                            "AND NOT granted AND classid = $1::oid AND objid = $2::oid "
                            "AND objsubid = $3",
                            lock["classid"],
                            lock["objid"],
                            lock["objsubid"],
                        )
                        if waiting:
                            break
                        await asyncio.sleep(0.01)
                    assert waiting >= 1
                    assert not task.done()

                await asyncio.wait_for(asyncio.shield(task), timeout=5)
            finally:
                if task is not None:
                    if not task.done():
                        task.cancel()
                    result = (await asyncio.gather(task, return_exceptions=True))[0]
                    if result is not None and not isinstance(result, BaseException):
                        await result.close()
                await observer.close()
                await blocker.close()


async def test_parallel_backend_connects_serialize_schema_bootstrap():
    """Concurrent workers and pods must not race in PostgreSQL catalogs."""
    import asyncpg

    from cdc.schema import tenancy

    async with fresh_tenant_db() as owner_dsn:
        results = await asyncio.gather(
            *(PostgresHitlBackend.connect(owner_dsn, TENANT_A) for _ in range(4)),
            *(PostgresRegistryBackend.connect(owner_dsn, TENANT_B) for _ in range(4)),
            return_exceptions=True,
        )
        backends = [result for result in results if not isinstance(result, BaseException)]
        try:
            failures = [result for result in results if isinstance(result, BaseException)]
            assert failures == []

            conn = await asyncpg.connect(dsn=owner_dsn)
            try:
                table_rows = await conn.fetch(
                    "SELECT tablename FROM pg_tables "
                    "WHERE schemaname = current_schema() AND tablename = ANY($1::text[])",
                    list(tenancy.TENANT_TABLES),
                )
                policy_rows = await conn.fetch(
                    "SELECT tablename, policyname FROM pg_policies "
                    "WHERE schemaname = current_schema()"
                )
                assert {row["tablename"] for row in table_rows} == set(tenancy.TENANT_TABLES)
                assert {(row["tablename"], row["policyname"]) for row in policy_rows} == {
                    (table, tenancy.policy_name(table)) for table in tenancy.TENANT_TABLES
                }
            finally:
                await conn.close()
        finally:
            await asyncio.gather(*(backend.close() for backend in backends))


async def test_fail_closed_a_session_with_no_tenant_reads_zero_rows_and_cannot_write():
    import asyncpg

    async with fresh_tenant_db() as owner_dsn:
        seed = await asyncpg.connect(dsn=owner_dsn)
        try:
            await _apply_full_schema(seed)
            await _set_tenant(seed, TENANT_A)
            await seed.execute(
                "INSERT INTO watchlist (mmsi, reason) VALUES (200800001, 'rls drill')"
            )
            await seed.execute(
                """
                INSERT INTO hitl_queue (id, trace_id, mmsi, ts, score, reasons, confidence)
                VALUES ('rls-1', 'trace-rls-1', 200800001, now(), 1.0, '[]'::jsonb, 1.0)
                """
            )
            assert await seed.fetchval("SELECT count(*) FROM watchlist") == 1
        finally:
            await seed.close()

        # A brand-new session that never sets app.tenant_id: zero rows from
        # every tenant table (the policy compares against NULL), and any write
        # is rejected by the policy's WITH CHECK. Postgres enforces both; no
        # application code is involved.
        blank = await asyncpg.connect(dsn=owner_dsn)
        try:
            from cdc.schema import tenancy

            for table in tenancy.TENANT_TABLES:
                count = await blank.fetchval(f"SELECT count(*) FROM {table}")  # nosec B608
                assert count == 0, f"fail-open leak: {table} returned {count} rows"
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await blank.execute(
                    "INSERT INTO watchlist (mmsi, reason) VALUES (200800002, 'no tenant')"
                )
        finally:
            await blank.close()


async def test_cross_tenant_reads_are_blocked_by_postgres_not_the_application():
    import asyncpg

    async with fresh_tenant_db() as owner_dsn:
        seed = await asyncpg.connect(dsn=owner_dsn)
        try:
            await _apply_full_schema(seed)
            await _set_tenant(seed, TENANT_A)
            await seed.execute(
                "INSERT INTO watchlist (mmsi, reason) VALUES (200800010, 'tenant A row')"
            )
            await _set_tenant(seed, TENANT_B)
            await seed.execute(
                "INSERT INTO watchlist (mmsi, reason) VALUES (200800020, 'tenant B row')"
            )

            # tenant A: own row only, and a TARGETED read of B's row (the
            # drill M-tenant-leak shape: A's session querying for B's key)
            # comes back empty at the database layer.
            await _set_tenant(seed, TENANT_A)
            rows = await seed.fetch("SELECT mmsi FROM watchlist ORDER BY mmsi")
            assert [r["mmsi"] for r in rows] == [200800010]
            stolen = await seed.fetchrow("SELECT * FROM watchlist WHERE mmsi = 200800020")
            assert stolen is None

            # and tenant B is symmetric
            await _set_tenant(seed, TENANT_B)
            rows = await seed.fetch("SELECT mmsi FROM watchlist ORDER BY mmsi")
            assert [r["mmsi"] for r in rows] == [200800020]
        finally:
            await seed.close()


async def test_hitl_backend_sessions_are_tenant_scoped_end_to_end():
    async with fresh_tenant_db() as owner_dsn:
        backend_a = await PostgresHitlBackend.connect(owner_dsn, TENANT_A)
        backend_b = await PostgresHitlBackend.connect(owner_dsn, TENANT_B)
        try:
            await backend_a.enqueue("trace-a-1", _out("trace-a-1"), TS)
            await backend_b.enqueue("trace-b-1", _out("trace-b-1"), TS)

            pending_a = await backend_a.pending()
            pending_b = await backend_b.pending()
            assert [r["trace_id"] for r in pending_a] == ["trace-a-1"]
            assert [r["trace_id"] for r in pending_b] == ["trace-b-1"]

            # labeling ACROSS tenants fails exactly like a missing trace: the
            # UPDATE matches zero rows because RLS filtered B's row out of A's
            # session before the application ever saw it.
            with pytest.raises(HitlTraceNotFound):
                await backend_a.label(
                    FeedbackIn(trace_id="trace-b-1", label="correct", reviewer="arun")
                )
            assert (
                await backend_b.label(
                    FeedbackIn(trace_id="trace-b-1", label="correct", reviewer="arun")
                )
                == 0
            )
        finally:
            await backend_a.close()
            await backend_b.close()


async def test_registry_backend_sessions_are_tenant_scoped_end_to_end():
    async with fresh_tenant_db() as owner_dsn:
        backend_a = await PostgresRegistryBackend.connect(owner_dsn, TENANT_A)
        backend_b = await PostgresRegistryBackend.connect(owner_dsn, TENANT_B)
        try:
            await backend_a.upsert_watchlist(200800030, {"reason": "tenant A watch"})
            assert [r["mmsi"] for r in await backend_a.list_watchlist()] == [200800030]
            assert await backend_b.list_watchlist() == []
            assert await backend_b.delete_watchlist(200800030) is False
            # A's row survived B's blind delete: RLS filtered it from B's DELETE
            assert [r["mmsi"] for r in await backend_a.list_watchlist()] == [200800030]
        finally:
            await backend_a.close()
            await backend_b.close()


async def test_registry_composite_keys_allow_same_business_key_per_tenant():
    """RLS must not turn a shared MMSI into a cross-tenant uniqueness error."""
    async with fresh_tenant_db() as owner_dsn:
        backend_a = await PostgresRegistryBackend.connect(owner_dsn, TENANT_A)
        backend_b = await PostgresRegistryBackend.connect(owner_dsn, TENANT_B)
        try:
            mmsi = 200800040
            await backend_a.upsert_vessel(mmsi, {"name": "A vessel"})
            await backend_b.upsert_vessel(mmsi, {"name": "B vessel"})
            assert (await backend_a.get_vessel(mmsi))["name"] == "A vessel"
            assert (await backend_b.get_vessel(mmsi))["name"] == "B vessel"

            await backend_a.upsert_watchlist(mmsi, {"reason": "A reason"})
            await backend_b.upsert_watchlist(mmsi, {"reason": "B reason"})
            assert (await backend_a.list_watchlist())[0]["reason"] == "A reason"
            assert (await backend_b.list_watchlist())[0]["reason"] == "B reason"

            await backend_a.upsert_sanction(mmsi, {"regime": "OFAC", "reference": "A"})
            await backend_b.upsert_sanction(mmsi, {"regime": "OFAC", "reference": "B"})
            await backend_a.delete_sanctions(mmsi)
            assert await backend_b.delete_sanctions(mmsi) == 1

            assert await backend_a.delete_watchlist(mmsi) is True
            assert await backend_b.delete_watchlist(mmsi) is True
        finally:
            await backend_a.close()
            await backend_b.close()


async def test_legacy_registry_rows_backfill_to_sentinel_before_key_migration():
    """A pre-P39 row must not be attributed to the tenant running migration."""
    async with fresh_tenant_db() as owner_dsn:
        import asyncpg

        conn = await asyncpg.connect(dsn=owner_dsn)
        try:
            await _create_legacy_registry_schema(conn)
            await _set_tenant(conn, TENANT_A)
            await conn.execute("INSERT INTO vessels (mmsi, name) VALUES (200800050, 'legacy')")

            # Normal startup must refuse the legacy contract with the reviewed
            # migration instruction, not fail earlier on the new index.
            with pytest.raises(RuntimeError, match="run the P39 migration"):
                await PostgresRegistryBackend.connect(owner_dsn, TENANT_A)

            from scripts.migrate_p39 import migrate

            # Exercise the real entrypoint, including its table/index ordering.
            await migrate(owner_dsn)

            # The migration session is tenant A, but the old row belongs to the
            # single-tenant sentinel and is visible only after switching there.
            assert await conn.fetchval("SELECT count(*) FROM vessels") == 0
            await _set_tenant(conn, "00000000-0000-0000-0000-000000000000")
            assert await conn.fetchval("SELECT tenant_id::text FROM vessels") == (
                "00000000-0000-0000-0000-000000000000"
            )
            pk = await conn.fetchval(
                """SELECT pg_get_constraintdef(oid)
                     FROM pg_constraint
                    WHERE conrelid = 'vessels'::regclass AND contype = 'p'"""
            )
            assert pk == "PRIMARY KEY (tenant_id, mmsi)"

            # A completed RLS schema still needs an unfiltered all-tenant
            # census. Grant the reviewed migration role BYPASSRLS, then prove
            # the transaction is idempotent without relying on filtered zeros.
            await _grant_owner_bypass_rls(owner_dsn)
            await migrate(owner_dsn)
            assert await conn.fetchval("SELECT count(*) FROM vessels") == 1
        finally:
            await conn.close()


async def test_migration_refuses_an_rls_filtered_legacy_census():
    """A non-bypass role must never approve a census that FORCE RLS hid."""
    async with fresh_tenant_db() as owner_dsn:
        import asyncpg

        from cdc.schema import ddl, tenancy
        from scripts.migrate_p39 import migrate

        conn = await asyncpg.connect(dsn=owner_dsn)
        try:
            await _create_legacy_registry_schema(conn, with_tenant_id=True)
            await conn.execute(
                "INSERT INTO vessels (tenant_id, mmsi, name) VALUES ($1::uuid, 200800060, 'A')",
                TENANT_A,
            )
            for table in ddl.CDC_TABLES:
                for stmt in tenancy.security_statements_for(table):
                    await conn.execute(stmt)
        finally:
            await conn.close()

        with pytest.raises(RuntimeError, match="BYPASSRLS or superuser"):
            await migrate(owner_dsn)


async def test_migration_approval_preserves_tenants_and_repairs_policy_drift(monkeypatch):
    async with fresh_tenant_db() as owner_dsn:
        import asyncpg

        from cdc.schema import tenancy
        from scripts.migrate_p39 import APPROVED_CENSUS_ENV, migrate

        conn = await asyncpg.connect(dsn=owner_dsn)
        try:
            await _create_legacy_registry_schema(conn, with_tenant_id=True)
            await conn.execute(
                "INSERT INTO vessels (tenant_id, mmsi, name) VALUES ($1::uuid, 200800070, 'A')",
                TENANT_A,
            )
        finally:
            await conn.close()

        monkeypatch.delenv(APPROVED_CENSUS_ENV, raising=False)
        with pytest.raises(RuntimeError, match="classify the printed per-tenant census"):
            await migrate(owner_dsn)

        monkeypatch.setenv(APPROVED_CENSUS_ENV, "1")
        await migrate(owner_dsn)
        await _grant_owner_bypass_rls(owner_dsn)

        conn = await asyncpg.connect(dsn=owner_dsn)
        try:
            await _set_tenant(conn, TENANT_A)
            assert (
                await conn.fetchval("SELECT tenant_id::text FROM vessels WHERE mmsi = 200800070")
                == TENANT_A
            )

            await conn.execute(f"DROP POLICY {tenancy.policy_name('vessels')} ON vessels")
            await conn.execute(
                f"CREATE POLICY {tenancy.policy_name('vessels')} ON vessels "
                "USING (true) WITH CHECK (true)"
            )
        finally:
            await conn.close()

        await migrate(owner_dsn)
        conn = await asyncpg.connect(dsn=owner_dsn)
        try:
            qual = await conn.fetchval(
                """SELECT qual FROM pg_policies
                     WHERE tablename = 'vessels' AND policyname = $1""",
                tenancy.policy_name("vessels"),
            )
            assert "current_setting" in qual and qual != "true"
            await conn.execute("CREATE POLICY vessels_unexpected ON vessels USING (true)")
        finally:
            await conn.close()

        with pytest.raises(RuntimeError, match="has 2 policies"):
            await migrate(owner_dsn)
