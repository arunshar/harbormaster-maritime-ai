"""Tenancy DDL module tests (Phase 5, gate 5.4): statement shapes, idempotent
forms, the pinned checksum, and the cross-package sentinel sync. The live-RLS
behavior these statements produce is tested against a real Postgres in
serving/tests/test_tenant_rls.py (RLS is database-enforced; a mock cannot
stand in for it)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.config import DEFAULT_TENANT_ID as SERVING_DEFAULT_TENANT_ID
from cdc.schema import ddl, tenancy

EXPECTATIONS = Path(__file__).parent.parent.parent / "mlops" / "fixtures" / "expectations.json"


def test_tenant_tables_cover_hitl_and_every_cdc_table():
    assert tenancy.TENANT_TABLES[0] == "hitl_queue"
    assert tenancy.TENANT_TABLES[1:] == ddl.CDC_TABLES


def test_default_tenant_sentinel_matches_serving_config():
    # cdc and serving each carry a copy (the serving wheel must not depend on
    # cdc at runtime); this is the sanctions_flag_id anti-drift convention.
    assert tenancy.DEFAULT_TENANT_ID == SERVING_DEFAULT_TENANT_ID


def test_statements_for_expands_backfills_contracts_and_enables_rls_in_order():
    stmts = tenancy.statements_for("watchlist")
    assert len(stmts) == 8
    add, backfill, default, not_null, primary_key, enable, force, policy = stmts
    assert add.startswith("ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS tenant_id uuid")
    assert "DEFAULT" not in add and "NOT NULL" not in add
    assert f"SET tenant_id = '{tenancy.DEFAULT_TENANT_ID}'::uuid" in backfill
    assert "WHERE tenant_id IS NULL" in backfill
    assert "current_setting('app.tenant_id', true)" in default
    assert not_null == "ALTER TABLE watchlist ALTER COLUMN tenant_id SET NOT NULL;"
    assert "ARRAY['tenant_id', 'mmsi']::text[]" in primary_key
    assert "ALTER TABLE watchlist ADD PRIMARY KEY (tenant_id, mmsi)" in primary_key
    assert enable == "ALTER TABLE watchlist ENABLE ROW LEVEL SECURITY;"
    assert force == "ALTER TABLE watchlist FORCE ROW LEVEL SECURITY;"
    assert "CREATE POLICY watchlist_tenant_isolation ON watchlist" in policy
    assert "DROP POLICY watchlist_tenant_isolation ON watchlist" in policy
    assert "WITH CHECK" in policy


def test_statements_for_rejects_a_non_tenant_table():
    with pytest.raises(ValueError, match="not a tenant table"):
        tenancy.statements_for("pg_shadow")


def test_policy_predicate_uses_missing_ok_current_setting():
    # missing_ok=true + NULLIF is what makes an unset session read ZERO rows
    # instead of raising; the strict form would error on every query.
    assert "current_setting('app.tenant_id', true)" in tenancy.POLICY_PREDICATE
    assert "NULLIF" in tenancy.POLICY_PREDICATE
    for table in tenancy.TENANT_TABLES:
        policy = tenancy.statements_for(table)[-1]
        assert tenancy.POLICY_PREDICATE in policy


def test_legacy_backfill_precedes_the_session_aware_default():
    add, backfill, default, not_null, *_ = tenancy.statements_for("hitl_queue")
    assert "DEFAULT" not in add
    assert tenancy.DEFAULT_TENANT_ID in backfill
    assert "current_setting('app.tenant_id', true)" in default
    assert tenancy.DEFAULT_TENANT_ID in default
    assert "SET NOT NULL" in not_null


def test_full_statements_cover_every_table():
    stmts = tenancy.statements()
    assert len(stmts) == 31
    for table in tenancy.TENANT_TABLES:
        assert any(f"CREATE POLICY {tenancy.policy_name(table)}" in s for s in stmts)


def test_runtime_statements_exclude_the_connector_sensitive_key_contract():
    for table in ddl.CDC_TABLES:
        runtime = tenancy.runtime_statements_for(table)
        assert not any("ADD PRIMARY KEY" in stmt for stmt in runtime)
        assert len(runtime) == len(tenancy.statements_for(table)) - 1


def test_structural_and_security_statements_partition_the_migration():
    for table in ddl.CDC_TABLES:
        assert tenancy.statements_for(table) == (
            *tenancy.structural_statements_for(table),
            *tenancy.security_statements_for(table),
        )
        structural = tenancy.structural_statements_for(table)
        security = tenancy.security_statements_for(table)
        assert not any("ROW LEVEL SECURITY" in stmt for stmt in structural)
        assert any("ENABLE ROW LEVEL SECURITY" in stmt for stmt in security)


async def test_p39_migration_refuses_an_active_debezium_slot():
    from scripts.migrate_p39 import _assert_slot_inactive

    class ActiveSlotConnection:
        async def fetchval(self, query, slot_name):
            assert "pg_replication_slots" in query
            assert slot_name == ddl.SLOT_NAME
            return True

    with pytest.raises(RuntimeError, match="replication slot.*is active"):
        await _assert_slot_inactive(ActiveSlotConnection())


def test_canonical_ddl_sha_matches_the_pinned_checksum():
    pinned = json.loads(EXPECTATIONS.read_text())["tenant_rls"]
    assert tenancy.ddl_sha256() == pinned["ddl_sha256"], (
        "the tenancy DDL changed; if intentional, update "
        "mlops/fixtures/expectations.json AND docs/phases/PHASE_5.md in the same commit"
    )
    assert pinned["policy_predicate"] == tenancy.POLICY_PREDICATE
    assert pinned["tables"] == list(tenancy.TENANT_TABLES)
