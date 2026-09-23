"""Human-in-the-loop review queue.

The Phase 1.2 backend is a real Postgres table read by the Streamlit reviewer
console; the verdict it captures is the seed of the Phase 4 RL-flywheel data.

Two backends behind one interface:
  - MemoryHitlBackend: hermetic, used by unit tests and when no DSN is set.
  - PostgresHitlBackend: asyncpg; INSERT ... ON CONFLICT (trace_id) DO NOTHING
    so an at-least-once redelivery does not write a duplicate review row.

Row schema (both backends mirror it):
  id, trace_id, mmsi, ts, score, reasons (jsonb), confidence, label, reviewer, created_at
"""

from __future__ import annotations

import json
import socket
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import uuid4

import structlog

from app.config import DEFAULT_TENANT_ID, Settings
from app.errors import HitlTraceNotFound
from app.models import AisScoreOut, FeedbackIn

log = structlog.get_logger(__name__)


def _row(trace_id: str, out: AisScoreOut, ts: datetime) -> dict[str, Any]:
    return {
        "id": uuid4().hex,
        "trace_id": trace_id,
        "mmsi": out.mmsi,
        "ts": ts,
        "score": out.score,
        "reasons": [r.model_dump(mode="json") for r in out.reasons],
        "confidence": out.confidence,
        "label": None,
        "reviewer": None,
        "created_at": datetime.now(UTC),
    }


class HitlBackend(Protocol):
    async def enqueue(self, trace_id: str, out: AisScoreOut, ts: datetime) -> None: ...
    async def label(self, payload: FeedbackIn) -> int: ...
    async def pending(self) -> list[dict[str, Any]]: ...
    async def close(self) -> None: ...


class MemoryHitlBackend:
    def __init__(self) -> None:
        self._rows: list[dict[str, Any]] = []
        self._seen: set[str] = set()

    async def enqueue(self, trace_id: str, out: AisScoreOut, ts: datetime) -> None:
        if trace_id in self._seen:  # idempotent on the replay-stable trace id
            return
        self._seen.add(trace_id)
        self._rows.append(_row(trace_id, out, ts))
        log.info("hitl_enqueue", trace_id=trace_id, mmsi=out.mmsi, score=out.score)

    async def label(self, payload: FeedbackIn) -> int:
        matched = False
        for r in self._rows:
            if r["trace_id"] == payload.trace_id:
                r["label"] = payload.label
                r["reviewer"] = payload.reviewer
                matched = True
                break
        if not matched:
            raise HitlTraceNotFound("no queued review for trace_id", trace_id=payload.trace_id)
        return sum(1 for r in self._rows if r["label"] is None)

    async def pending(self) -> list[dict[str, Any]]:
        return [r for r in self._rows if r["label"] is None]

    async def rows(self) -> list[dict[str, Any]]:
        return list(self._rows)

    async def close(self) -> None:
        return None


_DDL = """
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
"""


class PostgresHitlBackend:
    def __init__(self, pool: Any) -> None:
        self._pool = pool

    @staticmethod
    async def _init_conn(conn: Any) -> None:
        # asyncpg returns jsonb as raw text by default; this codec makes it encode
        # from / decode to Python objects, so `reasons` round-trips as a list[dict].
        await conn.set_type_codec(
            "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
        )

    @classmethod
    async def connect(cls, dsn: str, tenant_id: str = DEFAULT_TENANT_ID) -> PostgresHitlBackend:
        import asyncpg  # imported lazily; only needed for the real backend

        async def _set_tenant(conn: Any) -> None:
            # Phase 5 gate 5.4: pin app.tenant_id BEFORE any query. The RLS
            # policies (cdc/schema/tenancy.py) filter every read and stamp
            # every insert off this GUC; a session that skipped it would read
            # zero rows, fail-closed by Postgres, not by this code. This is
            # the pool's per-ACQUIRE setup hook, not the per-connection init:
            # asyncpg's release-time reset runs RESET ALL, which would wipe an
            # init-time GUC the first time a connection went back to the pool.
            await conn.execute("SELECT set_config('app.tenant_id', $1, false)", tenant_id)

        pool = await asyncpg.create_pool(
            dsn=dsn, min_size=1, max_size=4, init=cls._init_conn, setup=_set_tenant
        )
        try:
            async with pool.acquire() as conn:
                # CREATE TABLE IF NOT EXISTS can still race in PostgreSQL's
                # system catalogs. One transaction-scoped advisory lock makes
                # bootstrap safe across Uvicorn workers and separate pods.
                from cdc.schema import tenancy

                async with conn.transaction():
                    await conn.execute(
                        "SELECT pg_advisory_xact_lock($1)",
                        tenancy.SCHEMA_BOOTSTRAP_LOCK_ID,
                    )
                    await conn.execute(_DDL)
                    for stmt in tenancy.statements_for("hitl_queue"):
                        await conn.execute(stmt)
                    await tenancy.assert_security_contract(conn, "hitl_queue")
        except BaseException:
            await pool.close()
            raise
        return cls(pool)

    async def enqueue(self, trace_id: str, out: AisScoreOut, ts: datetime) -> None:
        r = _row(trace_id, out, ts)
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO hitl_queue
                    (id, trace_id, mmsi, ts, score, reasons, confidence, created_at)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
                ON CONFLICT (trace_id) DO NOTHING
                """,
                r["id"],
                r["trace_id"],
                r["mmsi"],
                r["ts"],
                r["score"],
                r["reasons"],
                r["confidence"],
                r["created_at"],
            )

    async def label(self, payload: FeedbackIn) -> int:
        async with self._pool.acquire() as conn:
            status = await conn.execute(
                "UPDATE hitl_queue SET label=$1, reviewer=$2 WHERE trace_id=$3",
                payload.label,
                payload.reviewer,
                payload.trace_id,
            )
            if status.endswith(" 0"):
                raise HitlTraceNotFound("no queued review for trace_id", trace_id=payload.trace_id)
            row = await conn.fetchrow("SELECT count(*) AS n FROM hitl_queue WHERE label IS NULL")
        return int(row["n"])

    async def pending(self) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM hitl_queue WHERE label IS NULL ORDER BY created_at"
            )
        return [dict(r) for r in rows]

    async def close(self) -> None:
        await self._pool.close()


class HitlQueue:
    """Facade that picks a backend and exposes the queue to the orchestrator."""

    def __init__(self, backend: HitlBackend) -> None:
        self.backend = backend

    @classmethod
    async def connect(cls, settings: Settings) -> HitlQueue:
        dsn = settings.resolved_pg_dsn()
        if dsn:
            try:
                backend: HitlBackend = await PostgresHitlBackend.connect(
                    dsn, settings.resolved_tenant_id()
                )
                log.info("hitl_backend", kind="postgres")
                return cls(backend)
            except ModuleNotFoundError:
                # Missing packaged runtime code is a broken image, not a
                # recoverable dependency outage.
                raise
            except Exception as exc:
                # Preserve the established development fallback only for a
                # connection outage. Schema, migration, and RLS failures must
                # stop startup instead of silently switching to memory.
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
                log.warning("hitl_postgres_unavailable_fallback_memory", err=str(exc))
        return cls(MemoryHitlBackend())

    async def enqueue(self, trace_id: str, out: AisScoreOut, ts: datetime) -> None:
        await self.backend.enqueue(trace_id, out, ts)

    async def label(self, payload: FeedbackIn) -> int:
        return await self.backend.label(payload)

    async def pending(self) -> list[dict[str, Any]]:
        return await self.backend.pending()

    async def close(self) -> None:
        await self.backend.close()
