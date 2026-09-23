"""Unit coverage for the explicit P39 composite-key migration guards."""

from __future__ import annotations

import pytest

from scripts import migrate_p39


class FetchConnection:
    def __init__(self, values=(), rows=()):
        self.values = iter(values)
        self.rows = rows

    async def fetchval(self, *_args):
        return next(self.values)

    async def fetch(self, *_args):
        return self.rows


async def test_migration_query_helpers_preserve_database_results():
    conn = FetchConnection(
        values=("PRIMARY KEY (tenant_id, mmsi)", True, True, True),
    )

    assert await migrate_p39._primary_key_definition(conn, "vessels") == (
        "PRIMARY KEY (tenant_id, mmsi)"
    )
    assert await migrate_p39._has_tenant_id(conn, "vessels") is True
    assert await migrate_p39._rls_enabled(conn, "vessels") is True
    assert await migrate_p39._role_can_bypass_rls(conn) is True


async def test_table_census_without_tenant_column():
    conn = FetchConnection(values=(3, False))

    assert await migrate_p39._table_census(conn, "vessels") == (3, (), False)


async def test_table_census_with_tenant_column():
    conn = FetchConnection(
        values=(3, True),
        rows=(
            {"tenant_id": "<null>", "n": 1},
            {"tenant_id": "tenant-a", "n": 2},
        ),
    )

    assert await migrate_p39._table_census(conn, "vessels") == (
        3,
        (("<null>", 1), ("tenant-a", 2)),
        True,
    )


@pytest.mark.parametrize(
    ("total", "census", "has_tenant_id", "expected"),
    (
        (0, (), False, ()),
        (2, (), False, (("sentinel", 2),)),
        (2, (("<null>", 2),), True, (("sentinel", 2),)),
        (
            3,
            (("<null>", 1), ("sentinel", 2)),
            True,
            (("sentinel", 3),),
        ),
        (1, (("tenant-a", 1),), True, (("tenant-a", 1),)),
    ),
)
def test_expected_post_census(total, census, has_tenant_id, expected):
    assert migrate_p39._expected_post_census(total, census, has_tenant_id, "sentinel") == expected


def _postcheck(**overrides):
    values = {
        "table": "vessels",
        "before_count": 2,
        "expected_census": (("tenant-a", 2),),
        "after_count": 2,
        "after_census": (("tenant-a", 2),),
        "null_count": 0,
        "actual_pk": "PRIMARY KEY (tenant_id, mmsi)",
    }
    values.update(overrides)
    migrate_p39._assert_postcheck(**values)


def test_postcheck_accepts_unchanged_rows_census_and_key():
    _postcheck()


@pytest.mark.parametrize(
    ("overrides", "message"),
    (
        ({"after_count": 1}, "row count changed"),
        ({"null_count": 1}, "retains 1 null tenant IDs"),
        ({"after_census": (("tenant-b", 2),)}, "tenant census changed"),
        ({"actual_pk": "PRIMARY KEY (mmsi)"}, "has 'PRIMARY KEY \\(mmsi\\)'"),
    ),
)
def test_postcheck_rejects_contract_drift(overrides, message):
    with pytest.raises(RuntimeError, match=message):
        _postcheck(**overrides)


def test_main_requires_confirmed_connector_stop(monkeypatch, capsys):
    monkeypatch.delenv("HM_P39_CONNECTOR_STOPPED", raising=False)

    assert migrate_p39.main() == 2
    assert "stop and drain Debezium" in capsys.readouterr().out


def test_main_requires_postgres_dsn(monkeypatch, capsys):
    monkeypatch.setenv("HM_P39_CONNECTOR_STOPPED", "1")
    monkeypatch.delenv("HM_PG_DSN", raising=False)

    assert migrate_p39.main() == 2
    assert "HM_PG_DSN is required" in capsys.readouterr().out


def test_main_runs_reviewed_migration(monkeypatch, capsys):
    calls = []

    async def fake_migrate(dsn):
        calls.append(dsn)

    monkeypatch.setenv("HM_P39_CONNECTOR_STOPPED", "1")
    monkeypatch.setenv("HM_PG_DSN", "postgresql://migration")
    monkeypatch.setattr(migrate_p39, "migrate", fake_migrate)

    assert migrate_p39.main() == 0
    assert calls == ["postgresql://migration"]
    output = capsys.readouterr().out
    assert "migration applied" in output
    assert "rebuild the tenant-qualified DynamoDB/Redis state" in output
