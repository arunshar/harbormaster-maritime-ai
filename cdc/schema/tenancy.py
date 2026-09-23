"""Multi-tenant `tenant_id` + row-level-security DDL (Phase 5, gate 5.4).

Single source of truth for the tenancy migration across BOTH Postgres planes:
the serving-plane `hitl_queue` (its base DDL lives in serving/app/hitl.py) and
the registry tables (base DDL in cdc/schema/ddl.py). Every statement here is
idempotent and re-runnable at backend connect, mirroring ddl.py exactly:
ADD COLUMN IF NOT EXISTS, re-runnable ALTER ... ROW LEVEL SECURITY, and a
guarded DO block for the policy (Postgres has no CREATE POLICY IF NOT EXISTS).

The isolation model (docs/phases/PHASE_5.md, locked decisions; DR-7):
- Every tenant table gains `tenant_id uuid NOT NULL`. New rows default to the
  session's `app.tenant_id` GUC, so no INSERT statement anywhere needs editing;
  a session that never set a tenant falls to the single-tenant sentinel for the
  column default but is then rejected by the policy's WITH CHECK, so an
  unattributed write cannot land.
- The policy predicate uses `current_setting('app.tenant_id', true)` (the
  missing_ok form) wrapped in NULLIF: a session with NO tenant context compares
  every row against NULL and reads ZERO rows. Fail-closed as a query result,
  not as an error, which is exactly the testable acceptance shape the gate
  names ("a session with no app.tenant_id set reads zero rows, never all
  rows"). The strict one-argument current_setting would instead raise on every
  query, which is not the contract the fail-closed drill asserts.
- ENABLE plus FORCE row level security: FORCE applies the policy to the table
  OWNER too, so a non-superuser app/owner role cannot bypass it. Postgres
  superusers and BYPASSRLS roles always bypass RLS by design; production and
  the gate tests therefore run isolation assertions as a non-superuser role,
  never as the bootstrap superuser.

The single-tenant sentinel keeps Phase 1-4 behavior byte-for-byte: with
Settings.tenant_id empty (the HM_PIDPM_ENDPOINT empty-disables convention),
every session pins the zero UUID and every row carries it, so all existing
flows read exactly what they wrote. DEFAULT_TENANT_ID is mirrored in
serving/app/config.py (the serving wheel must not depend on the cdc package at
runtime); a unit test asserts the two never drift.
"""

from __future__ import annotations

import hashlib

from cdc.schema.ddl import CDC_TABLES, DEFAULT_TENANT_ID

# hitl_queue first (serving plane), then the registry tables (CDC plane).
TENANT_TABLES: tuple[str, ...] = ("hitl_queue", *CDC_TABLES)

# The session GUC every Postgres-backed path sets before querying.
TENANT_GUC = "app.tenant_id"

# Serialize idempotent schema bootstrap across serving workers and replicas.
# 0x484D5F534348454D is the positive signed-bigint encoding of "HM_SCHEM";
# keep this stable because PostgreSQL advisory locks coordinate by integer key.
SCHEMA_BOOTSTRAP_LOCK_ID = 0x484D5F534348454D

# Column default: stamp the session's tenant on every insert; fall to the
# single-tenant sentinel only so the ALTER backfill can never NULL-violate
# (the policy's WITH CHECK still rejects a no-tenant session's writes).
_TENANT_DEFAULT = (
    f"COALESCE(NULLIF(current_setting('{TENANT_GUC}', true), '')::uuid, "
    f"'{DEFAULT_TENANT_ID}'::uuid)"
)

# The one predicate every policy uses; pinned in mlops/fixtures/expectations.json.
POLICY_PREDICATE = f"tenant_id = NULLIF(current_setting('{TENANT_GUC}', true), '')::uuid"

_COMPOSITE_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "vessels": ("tenant_id", "mmsi"),
    "watchlist": ("tenant_id", "mmsi"),
    "sanctions_flags": ("tenant_id", "id"),
}


def _primary_key_migration(table: str) -> str:
    target_columns = _COMPOSITE_PRIMARY_KEYS[table]
    business_key = target_columns[1]
    target_sql = ", ".join(target_columns)
    target_array = ", ".join(f"'{column}'" for column in target_columns)
    return f"""
DO $$
DECLARE
    current_pk_name text;
    current_pk_columns text[];
BEGIN
    SELECT constraint_row.conname,
           array_agg(attribute_row.attname ORDER BY key_column.ordinality)
      INTO current_pk_name, current_pk_columns
      FROM pg_constraint AS constraint_row
      CROSS JOIN LATERAL unnest(constraint_row.conkey)
          WITH ORDINALITY AS key_column(attnum, ordinality)
      JOIN pg_attribute AS attribute_row
        ON attribute_row.attrelid = constraint_row.conrelid
       AND attribute_row.attnum = key_column.attnum
     WHERE constraint_row.conrelid = '{table}'::regclass
       AND constraint_row.contype = 'p'
     GROUP BY constraint_row.conname;

    IF current_pk_columns = ARRAY[{target_array}]::text[] THEN
        NULL;
    ELSIF current_pk_columns = ARRAY['{business_key}']::text[] THEN
        EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I', '{table}', current_pk_name);
        ALTER TABLE {table} ADD PRIMARY KEY ({target_sql});
    ELSE
        RAISE EXCEPTION 'unexpected primary key on {table}: %', current_pk_columns;
    END IF;
END
$$;
""".strip()  # nosec B608  # table and key names come from fixed module-level mappings


def policy_name(table: str) -> str:
    return f"{table}_tenant_isolation"


def statements_for(table: str, *, include_primary_key_migration: bool = True) -> tuple[str, ...]:
    """The ordered, individually idempotent tenancy DDL for one table.

    Each plane applies its own tables at connect: serving/app/hitl.py runs
    this for hitl_queue, serving/app/registry.py for the CDC_TABLES, so
    neither plane needs the other's base tables to exist first.
    """
    if table not in TENANT_TABLES:
        raise ValueError(f"not a tenant table: {table!r} (expected one of {TENANT_TABLES})")
    # A session-derived DEFAULT on ADD COLUMN would stamp every legacy row with
    # whichever tenant runs the migration. Expand first, backfill the explicit
    # single-tenant sentinel, then install the session-aware write default.
    add_column = f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS tenant_id uuid;"
    backfill = (
        f"UPDATE {table} SET tenant_id = '{DEFAULT_TENANT_ID}'::uuid "  # nosec B608
        "WHERE tenant_id IS NULL;"
    )
    set_default = f"ALTER TABLE {table} ALTER COLUMN tenant_id SET DEFAULT {_TENANT_DEFAULT};"
    set_not_null = f"ALTER TABLE {table} ALTER COLUMN tenant_id SET NOT NULL;"
    migrate_primary_key = (
        (_primary_key_migration(table),)
        if include_primary_key_migration and table in _COMPOSITE_PRIMARY_KEYS
        else ()
    )
    enable_rls = f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;"
    force_rls = f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;"
    # Replace the named policy in one transactionally atomic statement. A
    # same-named drifted policy must not survive because permissive policies
    # are ORed by Postgres.
    policy = f"""
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_policies
        WHERE schemaname = current_schema()
          AND tablename = '{table}'
          AND policyname = '{policy_name(table)}'
    ) THEN
        DROP POLICY {policy_name(table)} ON {table};
    END IF;
    CREATE POLICY {policy_name(table)} ON {table}
        USING ({POLICY_PREDICATE})
        WITH CHECK ({POLICY_PREDICATE});
END
$$;
""".strip()  # nosec B608  # table names and the predicate are module-level constants, not untrusted input
    return (
        add_column,
        backfill,
        set_default,
        set_not_null,
        *migrate_primary_key,
        enable_rls,
        force_rls,
        policy,
    )


def runtime_statements_for(table: str) -> tuple[str, ...]:
    """Return connect-time tenancy DDL after the P39 key contract is verified.

    Replacing a Postgres primary key while Debezium is active can race its
    JDBC metadata. The destructive key contract is therefore an explicit
    migration-window operation, not an application-startup side effect.
    """
    return statements_for(table, include_primary_key_migration=False)


def structural_statements_for(table: str) -> tuple[str, ...]:
    """Column, backfill, and key DDL, before RLS is enabled."""
    return statements_for(table)[:-3]


def security_statements_for(table: str) -> tuple[str, ...]:
    """RLS enablement, enforcement, and policy DDL."""
    return statements_for(table, include_primary_key_migration=False)[-3:]


async def assert_security_contract(conn, table: str) -> None:
    """Refuse policy drift, including an extra permissive policy."""
    if table not in TENANT_TABLES:
        raise ValueError(f"not a tenant table: {table!r} (expected one of {TENANT_TABLES})")
    flags = await conn.fetchrow(
        """SELECT relrowsecurity, relforcerowsecurity
             FROM pg_class
            WHERE oid = $1::regclass""",
        table,
    )
    policies = await conn.fetch(
        """SELECT policyname, permissive, roles, cmd, qual, with_check
             FROM pg_policies
            WHERE schemaname = current_schema() AND tablename = $1""",
        table,
    )
    if not flags or not flags["relrowsecurity"] or not flags["relforcerowsecurity"]:
        raise RuntimeError(f"P39 postcheck failed: RLS is not forced on {table}")
    expected_name = policy_name(table)
    if len(policies) != 1:
        raise RuntimeError(
            f"P39 postcheck failed: {table} has {len(policies)} policies, expected one"
        )
    policy = policies[0]
    policy_shape_ok = (
        policy["policyname"] == expected_name
        and policy["permissive"] == "PERMISSIVE"
        and set(policy["roles"]) == {"public"}
        and policy["cmd"] == "ALL"
        and policy["qual"] == policy["with_check"]
        and "tenant_id" in (policy["qual"] or "")
        and "current_setting" in (policy["qual"] or "")
    )
    if not policy_shape_ok:
        raise RuntimeError(f"P39 postcheck failed: tenant policy drift on {table}")


def statements() -> tuple[str, ...]:
    """The full tenancy migration, all tables, in TENANT_TABLES order."""
    return tuple(stmt for table in TENANT_TABLES for stmt in statements_for(table))


def canonical_ddl() -> str:
    """The canonical tenancy DDL string the gate-5.4 checksum is taken over."""
    return "\n\n".join(statements()) + "\n"


def ddl_sha256() -> str:
    return hashlib.sha256(canonical_ddl().encode()).hexdigest()
