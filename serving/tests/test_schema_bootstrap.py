"""Unit guards for PostgreSQL schema-bootstrap lifecycle behavior."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from app.hitl import PostgresHitlBackend
from app.registry import PostgresRegistryBackend


class _AsyncContext:
    def __init__(self, value: Any) -> None:
        self._value = value

    async def __aenter__(self) -> Any:
        return self._value

    async def __aexit__(self, *_args: Any) -> None:
        return None


class _CancelledDdlConnection:
    def transaction(self) -> _AsyncContext:
        return _AsyncContext(self)

    async def execute(self, statement: str, *_args: Any) -> None:
        if "pg_advisory_xact_lock" not in statement:
            raise asyncio.CancelledError


class _Pool:
    def __init__(self) -> None:
        self.closed = False
        self._conn = _CancelledDdlConnection()

    def acquire(self) -> _AsyncContext:
        return _AsyncContext(self._conn)

    async def close(self) -> None:
        self.closed = True


@pytest.mark.parametrize("backend_cls", [PostgresHitlBackend, PostgresRegistryBackend])
async def test_bootstrap_cancellation_closes_new_pool(monkeypatch, backend_cls):
    import asyncpg

    pool = _Pool()

    async def create_pool(**_kwargs):
        return pool

    monkeypatch.setattr(asyncpg, "create_pool", create_pool)

    with pytest.raises(asyncio.CancelledError):
        await backend_cls.connect("postgresql://configured")

    assert pool.closed is True
