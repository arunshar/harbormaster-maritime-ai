"""Gate 5.4 smoke: RLS fail-closed + per-tenant burn-rate boundary case.

Usage: .venv/bin/python scripts/phase5_tenant_smoke.py
   or: make phase5-tenant-smoke

Two halves, zero AWS (docs/phases/PHASE_5.md gate 5.4 smoke criteria):
1. RLS fail-closed against a REAL local Postgres: with the tenancy DDL
   applied, a session that never sets app.tenant_id reads ZERO rows from
   every tenant table, and a tenant session cannot read another tenant's
   rows. Runs against HM_TEST_PG_DSN when set (any wal_level-agnostic
   Postgres works); otherwise spins a throwaway postgres:16 container the
   same way scripts/drill_p1_slot_bloat.py does. The isolation checks run
   as a throwaway NOSUPERUSER owner role (superusers bypass RLS by
   Postgres design, so asserting from the bootstrap user would be vacuous).
2. The per-tenant burn-rate boundary case: one sustained 3%-bad series,
   three tier verdicts (real-time PAGE, near-real-time WARNING, batch OK),
   asserted against the goldens pinned in mlops/fixtures/expectations.json.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess  # nosec B404  # docker lifecycle only, fixed argv, no shell
import sys
import time
import uuid
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "serving"))

from app.burn_rate import Bucket, evaluate_burn_rate  # noqa: E402
from app.slo_tenant import TenantTier, tier_success_target  # noqa: E402
from cdc.schema import ddl, tenancy  # noqa: E402

EXPECTATIONS = REPO_ROOT / "mlops" / "fixtures" / "expectations.json"

DOCKER_NAME = "hm-phase5-tenant-pg"
DOCKER_PORT = 55433  # drill_p1 uses 55432; a parallel run must not collide
DOCKER_DSN = f"postgresql://hm_admin:hm_local_pw@127.0.0.1:{DOCKER_PORT}/harbormaster"

TENANT_A = "11111111-1111-1111-1111-111111111111"
TENANT_B = "22222222-2222-2222-2222-222222222222"

_HITL_DDL = """
CREATE TABLE IF NOT EXISTS hitl_queue (
    id          TEXT PRIMARY KEY,
    trace_id    TEXT UNIQUE NOT NULL,
    mmsi        BIGINT NOT NULL,
    ts          TIMESTAMPTZ NOT NULL,
    score       DOUBLE PRECISION NOT NULL,
    reasons     JSONB NOT NULL,
    confidence  DOUBLE PRECISION NOT NULL,
    label       TEXT,
    reviewer    TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""  # mirrors serving/app/hitl.py's _DDL (kept local so the smoke needs no app import chain)


def _docker_pg_up() -> str:
    subprocess.run(["docker", "rm", "-f", DOCKER_NAME], capture_output=True, check=False)
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            DOCKER_NAME,
            "-e",
            "POSTGRES_DB=harbormaster",
            "-e",
            "POSTGRES_USER=hm_admin",
            "-e",
            "POSTGRES_PASSWORD=hm_local_pw",  # throwaway, container-local
            "-p",
            f"127.0.0.1:{DOCKER_PORT}:5432",
            "postgres:16-alpine",
        ],
        check=True,
        capture_output=True,
    )
    return DOCKER_DSN


def _docker_pg_down() -> None:
    subprocess.run(["docker", "rm", "-f", DOCKER_NAME], capture_output=True, check=False)


async def _wait_ready(dsn: str, timeout_s: float = 60.0) -> None:
    import asyncpg

    t0 = time.time()
    while True:
        try:
            conn = await asyncpg.connect(dsn=dsn, timeout=5)
            await conn.close()
            return
        except Exception:
            if time.time() - t0 > timeout_s:
                raise
            await asyncio.sleep(1.0)


def _swap_dsn(dsn: str, *, user: str, password: str, database: str) -> str:
    parts = urlsplit(dsn)
    host = parts.hostname or "127.0.0.1"
    port = f":{parts.port}" if parts.port else ""
    return urlunsplit((parts.scheme, f"{user}:{password}@{host}{port}", f"/{database}", "", ""))


async def rls_fail_closed_check(admin_dsn: str) -> list[str]:
    import asyncpg

    log: list[str] = []
    suffix = uuid.uuid4().hex[:10]
    role, db = f"hm_smoke_owner_{suffix}", f"hm_smoke_{suffix}"

    admin = await asyncpg.connect(dsn=admin_dsn)
    try:
        await admin.execute(
            f"CREATE ROLE {role} LOGIN PASSWORD 'hm_smoke_pw' NOSUPERUSER NOBYPASSRLS"
        )
        await admin.execute(f"CREATE DATABASE {db} OWNER {role}")
    finally:
        await admin.close()
    owner_dsn = _swap_dsn(admin_dsn, user=role, password="hm_smoke_pw", database=db)

    try:
        seed = await asyncpg.connect(dsn=owner_dsn)
        try:
            for stmt in ddl.statements():
                await seed.execute(stmt)
            await seed.execute(_HITL_DDL)
            for stmt in tenancy.statements():
                await seed.execute(stmt)
            log.append(
                f"tenancy DDL applied to {len(tenancy.TENANT_TABLES)} tables "
                f"(sha256 {tenancy.ddl_sha256()[:16]}...) as NOSUPERUSER owner `{role}`"
            )

            await seed.execute("SELECT set_config('app.tenant_id', $1, false)", TENANT_A)
            await seed.execute(
                "INSERT INTO vessels (mmsi, name) VALUES (200800001, 'phase5 vessel A')"
            )
            await seed.execute(
                "INSERT INTO watchlist (mmsi, reason) VALUES (200800001, 'phase5 smoke A')"
            )
            await seed.execute(
                """
                INSERT INTO sanctions_flags (id, mmsi, regime, reference)
                VALUES ('200800001:ofac', 200800001, 'ofac', 'phase5 A')
                """
            )
            await seed.execute(
                """
                INSERT INTO hitl_queue (id, trace_id, mmsi, ts, score, reasons, confidence)
                VALUES ('smoke-a', 'trace-smoke-a', 200800001, now(), 1.0, '[]'::jsonb, 1.0)
                """
            )
            for table in ("vessels", "watchlist", "sanctions_flags"):
                n = await seed.fetchval(f"SELECT count(*) FROM {table}")  # nosec B608
                assert n == 1, f"tenant A should see its own {table} row, saw {n}"
            log.append("tenant A seeded same-MMSI vessel, watchlist, sanctions, and HITL rows")

            await seed.execute("SELECT set_config('app.tenant_id', $1, false)", TENANT_B)
            # P39 regression: the same business key is valid independently in
            # both tenants. RLS must not turn B's upsert into a uniqueness error.
            await seed.execute(
                "INSERT INTO vessels (mmsi, name) VALUES (200800001, 'phase5 vessel B')"
            )
            await seed.execute(
                "INSERT INTO watchlist (mmsi, reason) VALUES (200800001, 'phase5 smoke B')"
            )
            await seed.execute(
                """
                INSERT INTO sanctions_flags (id, mmsi, regime, reference)
                VALUES ('200800001:ofac', 200800001, 'ofac', 'phase5 B')
                """
            )
            assert await seed.fetchval("SELECT count(*) FROM vessels") == 1
            assert await seed.fetchval("SELECT count(*) FROM watchlist") == 1
            assert await seed.fetchval("SELECT count(*) FROM sanctions_flags") == 1
            assert await seed.fetchval("SELECT name FROM vessels") == "phase5 vessel B"
            assert await seed.fetchval("SELECT reason FROM watchlist") == "phase5 smoke B"
            assert await seed.fetchval("SELECT reference FROM sanctions_flags") == "phase5 B"
            log.append(
                "P39 same-MMSI composite-key check passed for vessels, watchlist, and sanctions"
            )
            assert await seed.fetchval("SELECT count(*) FROM hitl_queue") == 0
            for table in ("hitl_queue",):
                n = await seed.fetchval(f"SELECT count(*) FROM {table}")  # nosec B608
                assert n == 0, f"CROSS-TENANT LEAK: tenant B read {n} rows from {table}"
            await seed.execute("SELECT set_config('app.tenant_id', $1, false)", TENANT_A)
            assert await seed.fetchval("SELECT name FROM vessels") == "phase5 vessel A"
            assert await seed.fetchval("SELECT reason FROM watchlist") == "phase5 smoke A"
            assert await seed.fetchval("SELECT reference FROM sanctions_flags") == "phase5 A"
            log.append(
                "tenant B reads its own same-MMSI row only; tenant A remains isolated "
                "(cross-tenant blocked by RLS)"
            )
        finally:
            await seed.close()

        blank = await asyncpg.connect(dsn=owner_dsn)
        try:
            for table in tenancy.TENANT_TABLES:
                n = await blank.fetchval(f"SELECT count(*) FROM {table}")  # nosec B608
                assert n == 0, f"FAIL-OPEN LEAK: no-tenant session read {n} rows from {table}"
            log.append(
                "FAIL-CLOSED CONFIRMED: a session with NO app.tenant_id set reads zero rows "
                f"from all {len(tenancy.TENANT_TABLES)} tenant tables (never all rows)"
            )
            try:
                await blank.execute(
                    "INSERT INTO watchlist (mmsi, reason) VALUES (200800099, 'no tenant')"
                )
                raise AssertionError("FAIL-OPEN WRITE: a no-tenant session inserted a row")
            except asyncpg.InsufficientPrivilegeError:
                log.append("no-tenant WRITE rejected by the policy's WITH CHECK (42501)")
        finally:
            await blank.close()
    finally:
        admin = await asyncpg.connect(dsn=admin_dsn)
        try:
            await admin.execute(f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")
            await admin.execute(f"DROP ROLE IF EXISTS {role}")
        finally:
            await admin.close()
    return log


def tenant_burn_boundary_check() -> list[str]:
    log: list[str] = []
    pinned = json.loads(EXPECTATIONS.read_text())["tenant_slo_tiers"]["boundary_case"]
    buckets = [Bucket(timestamp=float(i * 60), total=1000, bad=30) for i in range(1440)]
    for tier in TenantTier:
        result = evaluate_burn_rate(buckets, target=tier_success_target(tier), bucket_seconds=60.0)
        expected_status = pinned[f"{tier.value}_status"]
        expected_rollback = pinned[f"{tier.value}_should_rollback"]
        assert result.status.value == expected_status, (
            f"{tier.value}: status {result.status.value} != pinned {expected_status}"
        )
        assert result.should_rollback == expected_rollback
        log.append(
            f"  {tier.value} (target {tier_success_target(tier)}): "
            f"{result.status.value} rollback={result.should_rollback} [matches pin]"
        )
    log.append(
        "PER-TENANT BOUNDARY CONFIRMED: one 3%-bad series, three tier verdicts "
        "(page / warning / ok), all matching mlops/fixtures/expectations.json"
    )
    return log


def main() -> int:
    dsn = os.environ.get("HM_TEST_PG_DSN", "")
    started_docker = False
    try:
        if not dsn:
            print("no HM_TEST_PG_DSN; starting a throwaway postgres:16 container")
            dsn = _docker_pg_up()
            started_docker = True
        asyncio.run(_wait_ready(dsn))
        for line in asyncio.run(rls_fail_closed_check(dsn)):
            print(f"  {line}")
        for line in tenant_burn_boundary_check():
            print(line)
        print("[PASS] gate 5.4 smoke: RLS fail-closed + per-tenant burn-rate boundary")
        return 0
    except Exception as exc:
        print(f"[FAIL] {exc}")
        raise
    finally:
        if started_docker:
            _docker_pg_down()
            print("throwaway postgres container removed")


if __name__ == "__main__":
    sys.exit(main())
