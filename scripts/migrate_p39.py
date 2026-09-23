"""Apply the P39 Postgres key migration in a stopped-connector window.

This is intentionally explicit. Changing a primary key changes Debezium's
record key, so normal serving startup verifies the contract and never performs
the migration implicitly.
"""

from __future__ import annotations

import asyncio
import os
import sys

APPROVED_CENSUS_ENV = "HM_P39_APPROVE_EXISTING_TENANTS"

_EXPECTED_PRIMARY_KEYS = {
    "vessels": "PRIMARY KEY (tenant_id, mmsi)",
    "watchlist": "PRIMARY KEY (tenant_id, mmsi)",
    "sanctions_flags": "PRIMARY KEY (tenant_id, id)",
}


async def _assert_slot_inactive(conn) -> None:
    from cdc.schema import ddl

    active = await conn.fetchval(
        "SELECT active FROM pg_replication_slots WHERE slot_name = $1",
        ddl.SLOT_NAME,
    )
    if active:
        raise RuntimeError(
            f"refusing to migrate: replication slot {ddl.SLOT_NAME!r} is active; "
            "stop and drain Debezium first"
        )


async def _primary_key_definition(conn, table: str) -> str | None:
    return await conn.fetchval(
        """SELECT pg_get_constraintdef(oid)
             FROM pg_constraint
            WHERE conrelid = $1::regclass AND contype = 'p'""",
        table,
    )


async def _has_tenant_id(conn, table: str) -> bool:
    return bool(
        await conn.fetchval(
            """SELECT EXISTS (
                   SELECT 1 FROM information_schema.columns
                    WHERE table_schema = current_schema()
                      AND table_name = $1
                      AND column_name = 'tenant_id'
               )""",
            table,
        )
    )


async def _rls_enabled(conn, table: str) -> bool:
    return bool(
        await conn.fetchval(
            "SELECT relrowsecurity FROM pg_class WHERE oid = $1::regclass",
            table,
        )
    )


async def _role_can_bypass_rls(conn) -> bool:
    return bool(
        await conn.fetchval(
            "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user"
        )
    )


async def _table_census(conn, table: str) -> tuple[int, tuple[tuple[str, int], ...], bool]:
    total = int(await conn.fetchval(f"SELECT count(*) FROM {table}"))  # nosec B608
    has_tenant_id = await _has_tenant_id(conn, table)
    if not has_tenant_id:
        return total, (), False
    rows = await conn.fetch(
        f"""SELECT COALESCE(tenant_id::text, '<null>') AS tenant_id, count(*) AS n
              FROM {table}
             GROUP BY tenant_id
             ORDER BY tenant_id NULLS FIRST"""  # nosec B608
    )
    return total, tuple((str(row["tenant_id"]), int(row["n"])) for row in rows), True


def _expected_post_census(
    total: int,
    census: tuple[tuple[str, int], ...],
    has_tenant_id: bool,
    sentinel: str,
) -> tuple[tuple[str, int], ...]:
    if not has_tenant_id:
        return ((sentinel, total),) if total else ()
    counts = dict(census)
    null_rows = counts.pop("<null>", 0)
    if null_rows:
        counts[sentinel] = counts.get(sentinel, 0) + null_rows
    return tuple(sorted(counts.items()))


def _assert_postcheck(
    table: str,
    *,
    before_count: int,
    expected_census: tuple[tuple[str, int], ...],
    after_count: int,
    after_census: tuple[tuple[str, int], ...],
    null_count: int,
    actual_pk: str | None,
) -> None:
    if after_count != before_count:
        raise RuntimeError(
            f"P39 postcheck failed: {table} row count changed from {before_count} to {after_count}"
        )
    if null_count:
        raise RuntimeError(f"P39 postcheck failed: {table} retains {null_count} null tenant IDs")
    if after_census != expected_census:
        raise RuntimeError(
            f"P39 postcheck failed: {table} tenant census changed from "
            f"{expected_census!r} to {after_census!r}"
        )
    if actual_pk != _EXPECTED_PRIMARY_KEYS[table]:
        raise RuntimeError(
            f"P39 postcheck failed: {table} has {actual_pk!r}, "
            f"expected {_EXPECTED_PRIMARY_KEYS[table]!r}"
        )


async def migrate(dsn: str) -> None:
    import asyncpg

    from cdc.schema import ddl, tenancy

    conn = await asyncpg.connect(dsn=dsn)
    try:
        async with conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock($1)",
                tenancy.SCHEMA_BOOTSTRAP_LOCK_ID,
            )
            await _assert_slot_inactive(conn)
            # CREATE TABLE is a no-op on a legacy schema. Add/backfill tenant_id
            # before creating the new tenant-qualified sanctions index.
            for stmt in ddl.table_statements():
                await conn.execute(stmt)

            before_counts: dict[str, int] = {}
            expected_census: dict[str, tuple[tuple[str, int], ...]] = {}
            approval_required: list[str] = []
            metadata: dict[str, tuple[str | None, bool, bool]] = {}
            for table in ddl.CDC_TABLES:
                current_pk = await _primary_key_definition(conn, table)
                has_tenant_id = await _has_tenant_id(conn, table)
                rls_enabled = await _rls_enabled(conn, table)
                metadata[table] = (current_pk, has_tenant_id, rls_enabled)

            rls_tables = [table for table, (_, _, rls_enabled) in metadata.items() if rls_enabled]
            if rls_tables:
                if not await _role_can_bypass_rls(conn):
                    joined = ", ".join(rls_tables)
                    raise RuntimeError(
                        f"refusing to migrate {joined}: FORCE RLS can hide the tenant census; "
                        "use a reviewed BYPASSRLS or superuser migration role"
                    )
                await conn.execute("SET LOCAL row_security = off")

            for table in ddl.CDC_TABLES:
                current_pk, _, _ = metadata[table]
                total, census, has_tenant_id = await _table_census(conn, table)
                before_counts[table] = total
                expected_census[table] = _expected_post_census(
                    total,
                    census,
                    has_tenant_id,
                    ddl.DEFAULT_TENANT_ID,
                )
                census_text = ", ".join(f"{tenant}={n}" for tenant, n in census)
                print(
                    f"P39 preflight {table}: rows={total}; "
                    f"tenant_census={census_text or '<column absent or empty>'}"
                )
                if has_tenant_id and current_pk != _EXPECTED_PRIMARY_KEYS[table]:
                    approval_required.append(table)

            if approval_required and os.environ.get(APPROVED_CENSUS_ENV) != "1":
                joined = ", ".join(approval_required)
                raise RuntimeError(
                    f"refusing to migrate {joined}: tenant_id already exists under a legacy key; "
                    "classify the printed per-tenant census, then set "
                    f"{APPROVED_CENSUS_ENV}=1 for the reviewed migration window"
                )

            for table in ddl.CDC_TABLES:
                for stmt in tenancy.structural_statements_for(table):
                    await conn.execute(stmt)

            for table in ddl.CDC_TABLES:
                after_count, after_census, _ = await _table_census(conn, table)
                null_count = int(
                    await conn.fetchval(
                        f"SELECT count(*) FROM {table} WHERE tenant_id IS NULL"  # nosec B608
                    )
                )
                actual_pk = await _primary_key_definition(conn, table)
                _assert_postcheck(
                    table,
                    before_count=before_counts[table],
                    expected_census=expected_census[table],
                    after_count=after_count,
                    after_census=after_census,
                    null_count=null_count,
                    actual_pk=actual_pk,
                )

            for stmt in ddl.post_tenancy_statements():
                await conn.execute(stmt)
            for table in ddl.CDC_TABLES:
                for stmt in tenancy.security_statements_for(table):
                    await conn.execute(stmt)
                await tenancy.assert_security_contract(conn, table)
            await _assert_slot_inactive(conn)
    finally:
        await conn.close()


def main() -> int:
    if os.environ.get("HM_P39_CONNECTOR_STOPPED") != "1":
        print(
            "refusing to migrate: stop and drain Debezium, then set "
            "HM_P39_CONNECTOR_STOPPED=1 for this reviewed window"
        )
        return 2
    dsn = os.environ.get("HM_PG_DSN")
    if not dsn:
        print("HM_PG_DSN is required")
        return 2
    asyncio.run(migrate(dsn))
    print("P39 composite-key migration applied")
    print(
        "Postgres only: rebuild the tenant-qualified DynamoDB/Redis state with a verified "
        "snapshot or full replay before resuming serving; see "
        "docs/runbooks/P39_COMPOSITE_KEY_MIGRATION.md"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
