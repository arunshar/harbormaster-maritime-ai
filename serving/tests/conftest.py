"""Shared fixtures for the serving test suite."""

from __future__ import annotations

import pytest

from app.config import Settings
from app.orchestrator import Orchestrator
from replay.loader import load_expectations, load_fixture


@pytest.fixture
async def orch():
    o = await Orchestrator.bootstrap(Settings())  # memory HITL (no DSN)
    yield o
    await o.shutdown()


@pytest.fixture(scope="session")
def fixture_by_mmsi():
    by: dict[int, list] = {}
    for r in load_fixture():
        by.setdefault(r.mmsi, []).append(r)
    for m in by:
        by[m].sort(key=lambda r: r.t)
    return by


@pytest.fixture(scope="session")
def expectations():
    return load_expectations()
