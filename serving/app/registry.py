"""Analyst-edited registry: vessels, watchlist, sanctions_flags (Phase 2, gate C1).

Postgres is the system of record; the serving API writes ONLY here. The online
store (DynamoDB + Redis) is CDC-fed by cdc/consumer and is read by the scorer's
WatchlistLookup; nothing in this module touches it. That separation is the whole
point of the phase: freshness of the online copy is Debezium's job, not the API's.

Two backends behind one interface, mirroring serving/app/hitl.py exactly:
  - MemoryRegistryBackend: hermetic, used by unit tests and when no DSN is set.
  - PostgresRegistryBackend: asyncpg; runs the idempotent cdc/schema DDL at
    connect (tables + REPLICA IDENTITY FULL + the harbormaster_cdc publication),
    upserts via INSERT ... ON CONFLICT DO UPDATE.
"""

from __future__ import annotations

import socket
from datetime import UTC, datetime
from typing import Any, Protocol

import structlog

from app.config import DEFAULT_TENANT_ID, Settings
from app.errors import RegistryEntryNotFound

log = structlog.get_logger(__name__)


def _public_row(row: Any) -> dict[str, Any]:
    """Return a registry row without exposing the internal tenancy key."""
    result = dict(row)
    result.pop("tenant_id", None)
    return result


def _sanctions_flag_id(mmsi: int, regime: str) -> str:
    # Kept in sync with cdc.schema.ddl.sanctions_flag_id by a unit test. A blank
    # regime would mint the "<mmsi>:" id the CDC key mapper rejects (a poison
    # event); refuse it here too, defense in depth behind the model validator.
    normalized = regime.strip().lower()
    if not normalized:
        raise ValueError("regime must not be blank")
    return f"{int(mmsi)}:{normalized}"


class RegistryBackend(Protocol):
    async def upsert_vessel(self, mmsi: int, fields: dict[str, str]) -> dict[str, Any]: ...
    async def get_vessel(self, mmsi: int) -> dict[str, Any] | None: ...
    async def upsert_watchlist(self, mmsi: int, fields: dict[str, Any]) -> dict[str, Any]: ...
    async def delete_watchlist(self, mmsi: int) -> bool: ...
    async def list_watchlist(self) -> list[dict[str, Any]]: ...
    async def upsert_sanction(self, mmsi: int, fields: dict[str, str]) -> dict[str, Any]: ...
    async def delete_sanctions(self, mmsi: int) -> int: ...
    async def close(self) -> None: ...


class MemoryRegistryBackend:
    def __init__(self, tenant_id: str = DEFAULT_TENANT_ID) -> None:
        self._tenant_id = tenant_id
        self._vessels: dict[int, dict[str, Any]] = {}
        self._watchlist: dict[int, dict[str, Any]] = {}
        self._sanctions: dict[str, dict[str, Any]] = {}

    async def upsert_vessel(self, mmsi: int, fields: dict[str, str]) -> dict[str, Any]:
        row = {
            "tenant_id": self._tenant_id,
            "mmsi": mmsi,
            "name": fields.get("name", ""),
            "flag_state": fields.get("flag_state", ""),
            "vessel_type": fields.get("vessel_type", ""),
            "updated_at": datetime.now(UTC),
        }
        self._vessels[mmsi] = row
        return _public_row(row)

    async def get_vessel(self, mmsi: int) -> dict[str, Any] | None:
        row = self._vessels.get(mmsi)
        return _public_row(row) if row else None

    async def upsert_watchlist(self, mmsi: int, fields: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now(UTC)
        prev = self._watchlist.get(mmsi)
        row = {
            "tenant_id": self._tenant_id,
            "mmsi": mmsi,
            "reason": fields["reason"],
            "severity": float(fields.get("severity", 0.9)),
            "added_by": fields.get("added_by", ""),
            "created_at": prev["created_at"] if prev else now,
            "updated_at": now,
        }
        self._watchlist[mmsi] = row
        return _public_row(row)

    async def delete_watchlist(self, mmsi: int) -> bool:
        return self._watchlist.pop(mmsi, None) is not None

    async def list_watchlist(self) -> list[dict[str, Any]]:
        return [_public_row(r) for r in sorted(self._watchlist.values(), key=lambda r: r["mmsi"])]

    async def upsert_sanction(self, mmsi: int, fields: dict[str, str]) -> dict[str, Any]:
        now = datetime.now(UTC)
        fid = _sanctions_flag_id(mmsi, fields["regime"])
        prev = self._sanctions.get(fid)
        row = {
            "tenant_id": self._tenant_id,
            "id": fid,
            "mmsi": mmsi,
            "regime": fields["regime"],
            "reference": fields.get("reference", ""),
            "created_at": prev["created_at"] if prev else now,
            "updated_at": now,
        }
        self._sanctions[fid] = row
        return _public_row(row)

    async def delete_sanctions(self, mmsi: int) -> int:
        gone = [k for k, v in self._sanctions.items() if v["mmsi"] == mmsi]
        for k in gone:
            del self._sanctions[k]
        return len(gone)

    async def close(self) -> None:
        return None


class PostgresRegistryBackend:
    def __init__(self, pool: Any, tenant_id: str) -> None:
        self._pool = pool
        self._tenant_id = tenant_id

    @classmethod
    async def connect(cls, dsn: str, tenant_id: str = DEFAULT_TENANT_ID) -> PostgresRegistryBackend:
        import asyncpg  # lazy; only the real backend needs it

        from cdc.schema import ddl, tenancy  # lazy; only the Postgres path needs schema DDL

        async def _set_tenant(conn: Any) -> None:
            # Phase 5 gate 5.4: pin app.tenant_id before any query (see
            # hitl.py: per-acquire setup, not init, because asyncpg's
            # release-time RESET ALL would wipe an init-time GUC).
            await conn.execute("SELECT set_config('app.tenant_id', $1, false)", tenant_id)

        pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2, setup=_set_tenant)
        try:
            async with pool.acquire() as conn:
                # Use the same transaction-scoped lock as the HITL backend so
                # every schema bootstrap path serializes across processes.
                async with conn.transaction():
                    await conn.execute(
                        "SELECT pg_advisory_xact_lock($1)",
                        tenancy.SCHEMA_BOOTSTRAP_LOCK_ID,
                    )
                    # Table creation is safe on both fresh and legacy schemas.
                    # Verify the key contract before tenant-dependent index or
                    # RLS DDL so a legacy deployment gets the migration
                    # instruction instead of an UndefinedColumn error.
                    for stmt in ddl.table_statements():
                        await conn.execute(stmt)
                    for table, expected in (
                        ("vessels", "PRIMARY KEY (tenant_id, mmsi)"),
                        ("watchlist", "PRIMARY KEY (tenant_id, mmsi)"),
                        ("sanctions_flags", "PRIMARY KEY (tenant_id, id)"),
                    ):
                        actual = await conn.fetchval(
                            """SELECT pg_get_constraintdef(oid)
                                 FROM pg_constraint
                                WHERE conrelid = $1::regclass AND contype = 'p'""",
                            table,
                        )
                        if actual != expected:
                            raise RuntimeError(
                                f"{table} still has {actual!r}; run the P39 migration with "
                                "Debezium stopped before starting the registry"
                            )
                    for table in ddl.CDC_TABLES:
                        for stmt in tenancy.runtime_statements_for(table):
                            await conn.execute(stmt)
                        await tenancy.assert_security_contract(conn, table)
                    for stmt in ddl.post_tenancy_statements():
                        await conn.execute(stmt)
        except BaseException:
            await pool.close()
            raise
        return cls(pool, tenant_id)

    async def upsert_vessel(self, mmsi: int, fields: dict[str, str]) -> dict[str, Any]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO vessels (tenant_id, mmsi, name, flag_state, vessel_type, updated_at)
                VALUES ($1::uuid, $2, $3, $4, $5, now())
                ON CONFLICT (tenant_id, mmsi) DO UPDATE SET
                    name = EXCLUDED.name,
                    flag_state = EXCLUDED.flag_state,
                    vessel_type = EXCLUDED.vessel_type,
                    updated_at = now()
                RETURNING *
                """,
                self._tenant_id,
                mmsi,
                fields.get("name", ""),
                fields.get("flag_state", ""),
                fields.get("vessel_type", ""),
            )
        return _public_row(row)

    async def get_vessel(self, mmsi: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM vessels WHERE tenant_id = $1::uuid AND mmsi = $2",
                self._tenant_id,
                mmsi,
            )
        return _public_row(row) if row else None

    async def upsert_watchlist(self, mmsi: int, fields: dict[str, Any]) -> dict[str, Any]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO watchlist
                    (tenant_id, mmsi, reason, severity, added_by, created_at, updated_at)
                VALUES ($1::uuid, $2, $3, $4, $5, now(), now())
                ON CONFLICT (tenant_id, mmsi) DO UPDATE SET
                    reason = EXCLUDED.reason,
                    severity = EXCLUDED.severity,
                    added_by = EXCLUDED.added_by,
                    updated_at = now()
                RETURNING *
                """,
                self._tenant_id,
                mmsi,
                fields["reason"],
                float(fields.get("severity", 0.9)),
                fields.get("added_by", ""),
            )
        return _public_row(row)

    async def delete_watchlist(self, mmsi: int) -> bool:
        async with self._pool.acquire() as conn:
            status = await conn.execute(
                "DELETE FROM watchlist WHERE tenant_id = $1::uuid AND mmsi = $2",
                self._tenant_id,
                mmsi,
            )
        return not status.endswith(" 0")

    async def list_watchlist(self) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM watchlist WHERE tenant_id = $1::uuid ORDER BY mmsi",
                self._tenant_id,
            )
        return [_public_row(r) for r in rows]

    async def upsert_sanction(self, mmsi: int, fields: dict[str, str]) -> dict[str, Any]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO sanctions_flags
                    (tenant_id, id, mmsi, regime, reference, created_at, updated_at)
                VALUES ($1::uuid, $2, $3, $4, $5, now(), now())
                ON CONFLICT (tenant_id, id) DO UPDATE SET
                    reference = EXCLUDED.reference,
                    updated_at = now()
                RETURNING *
                """,
                self._tenant_id,
                _sanctions_flag_id(mmsi, fields["regime"]),
                mmsi,
                fields["regime"],
                fields.get("reference", ""),
            )
        return _public_row(row)

    async def delete_sanctions(self, mmsi: int) -> int:
        async with self._pool.acquire() as conn:
            status = await conn.execute(
                "DELETE FROM sanctions_flags WHERE tenant_id = $1::uuid AND mmsi = $2",
                self._tenant_id,
                mmsi,
            )
        try:
            return int(status.rsplit(" ", 1)[-1])
        except ValueError:  # pragma: no cover - asyncpg always returns "DELETE n"
            return 0

    async def close(self) -> None:
        await self._pool.close()


class RegistryStore:
    """Facade that picks a backend and exposes the registry to the API routes."""

    def __init__(self, backend: RegistryBackend) -> None:
        self.backend = backend

    @classmethod
    async def connect(cls, settings: Settings) -> RegistryStore:
        dsn = settings.resolved_pg_dsn()
        if dsn:
            try:
                backend: RegistryBackend = await PostgresRegistryBackend.connect(
                    dsn, settings.resolved_tenant_id()
                )
                log.info("registry_backend", kind="postgres")
                return cls(backend)
            except ModuleNotFoundError:
                # A configured durable backend must not silently become an
                # in-memory writer because the production image is incomplete.
                raise
            except Exception as exc:
                # A network outage keeps the established dev fallback. Schema,
                # migration, and RLS failures must surface instead of silently
                # bypassing Postgres and CDC with an in-memory writer.
                try:
                    import asyncpg

                    connection_error = isinstance(
                        exc,
                        (
                            ConnectionError,
                            TimeoutError,
                            socket.gaierror,
                            asyncpg.PostgresConnectionError,
                        ),
                    )
                except ModuleNotFoundError:
                    connection_error = False
                if not connection_error:
                    raise
                log.warning("registry_postgres_unavailable_fallback_memory", err=str(exc))
        return cls(MemoryRegistryBackend(settings.resolved_tenant_id()))

    async def upsert_vessel(self, mmsi: int, fields: dict[str, str]) -> dict[str, Any]:
        return await self.backend.upsert_vessel(mmsi, fields)

    async def get_vessel(self, mmsi: int) -> dict[str, Any]:
        row = await self.backend.get_vessel(mmsi)
        if row is None:
            raise RegistryEntryNotFound("vessel is not in the registry", mmsi=mmsi)
        return row

    async def upsert_watchlist(self, mmsi: int, fields: dict[str, Any]) -> dict[str, Any]:
        return await self.backend.upsert_watchlist(mmsi, fields)

    async def delete_watchlist(self, mmsi: int) -> None:
        if not await self.backend.delete_watchlist(mmsi):
            raise RegistryEntryNotFound("vessel is not on the watchlist", mmsi=mmsi)

    async def list_watchlist(self) -> list[dict[str, Any]]:
        return await self.backend.list_watchlist()

    async def upsert_sanction(self, mmsi: int, fields: dict[str, str]) -> dict[str, Any]:
        return await self.backend.upsert_sanction(mmsi, fields)

    async def delete_sanctions(self, mmsi: int) -> int:
        n = await self.backend.delete_sanctions(mmsi)
        if n == 0:
            raise RegistryEntryNotFound("vessel has no sanctions flags", mmsi=mmsi)
        return n

    async def close(self) -> None:
        await self.backend.close()
