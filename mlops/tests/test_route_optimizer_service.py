"""Gate 5.7: the FastAPI stretch service + the STRUCTURAL isolation pin.

Acceptance criterion (g): the PPO stretch never appears in the core promotion
pipeline. This test proves that by scanning the real import graph, not by
convention: mlops/route_optimizer/*.py must not import promote or the serving
orchestrator, and neither of those may import route_optimizer.
"""

from __future__ import annotations

import ast
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

import pytest
from fastapi.testclient import TestClient

from mlops.route_optimizer.ppo import TabularPolicy
from mlops.route_optimizer.service import create_app

_REPO = Path(__file__).resolve().parents[2]
_RO_DIR = _REPO / "mlops" / "route_optimizer"
_CORE_FILES = [
    _REPO / "mlops" / "promote.py",
    _REPO / "serving" / "app" / "orchestrator.py",
]


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    mods: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            mods.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            mods.add(node.module)
            # also the bound names: `from app import orchestrator` must be
            # recorded as `app.orchestrator`, not just `app`, or a leak written
            # that way would slip past the forbidden-set check.
            mods.update(f"{node.module}.{a.name}" for a in node.names)
    return mods


def test_route_optimizer_never_imports_core_pipeline():
    forbidden = ("mlops.promote", "app.orchestrator", "orchestrator")
    for py in _RO_DIR.glob("*.py"):
        mods = _imported_modules(py)
        for f in forbidden:
            assert not any(m == f or m.endswith(f".{f}") for m in mods), (
                f"{py.name} imports {f}: the stretch must stay out of the core pipeline"
            )


def test_core_pipeline_never_imports_route_optimizer():
    for core in _CORE_FILES:
        if not core.exists():
            continue
        mods = _imported_modules(core)
        assert not any("route_optimizer" in m for m in mods), (
            f"{core.name} imports route_optimizer: the core path must not depend on the stretch"
        )


def test_healthz_reports_toggle_state():
    on = TestClient(create_app(enabled=True))
    off = TestClient(create_app(enabled=False))
    assert on.get("/healthz").json()["enabled"] is True
    assert off.get("/healthz").json() == {
        "status": "ok",
        "service": "harbormaster-route-optimizer",
        "enabled": False,
    }


def test_disabled_service_returns_503():
    client = TestClient(create_app(enabled=False))
    resp = client.post("/v1/optimize-route", json={"start_node": "n0"})
    assert resp.status_code == 503
    assert "disabled" in resp.json()["detail"]


def test_enabled_optimizes_synthetic_demo_graph():
    client = TestClient(create_app(enabled=True))
    resp = client.post(
        "/v1/optimize-route",
        json={"start_node": "n0", "horizon": 6, "train_steps": 2, "n_rollouts": 4},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["graph"] == "synthetic-demo"
    assert body["route"][0] == "n0"
    assert body["n_hops"] == max(0, len(body["route"]) - 1)
    assert body["trained_steps"] == 2
    assert isinstance(body["reward"], float)


def test_training_request_does_not_block_healthz(monkeypatch):
    entered = Event()
    release = Event()

    def held_train_optimizer(graph, _cfg, **_kwargs):
        entered.set()
        assert release.wait(timeout=5.0), "test did not release fake optimizer"
        return TabularPolicy(graph.n_nodes, graph.max_out_degree), []

    monkeypatch.setattr(
        "mlops.route_optimizer.service.train_optimizer",
        held_train_optimizer,
    )
    with TestClient(create_app(enabled=True)) as client:
        with ThreadPoolExecutor(max_workers=2) as pool:
            optimize = pool.submit(
                client.post,
                "/v1/optimize-route",
                json={
                    "start_node": "n0",
                    "train_steps": 1,
                    "n_rollouts": 1,
                    "horizon": 1,
                },
            )
            try:
                assert entered.wait(timeout=1.0), "fake optimizer was not entered"
                assert not optimize.done()
                health = pool.submit(client.get, "/healthz")
                health_response = health.result(timeout=1.0)
            finally:
                release.set()
            optimize_response = optimize.result(timeout=2.0)

    assert health_response.status_code == 200
    assert health_response.json()["status"] == "ok"
    assert optimize_response.status_code == 200


def test_training_failure_returns_500(monkeypatch):
    def fail_train_optimizer(*_args, **_kwargs):
        raise RuntimeError("synthetic trainer failure")

    monkeypatch.setattr(
        "mlops.route_optimizer.service.train_optimizer",
        fail_train_optimizer,
    )
    with TestClient(
        create_app(enabled=True),
        raise_server_exceptions=False,
    ) as client:
        response = client.post(
            "/v1/optimize-route",
            json={
                "start_node": "n0",
                "train_steps": 1,
                "n_rollouts": 1,
                "horizon": 1,
            },
        )

    assert response.status_code == 500
    assert response.text == "Internal Server Error"


def test_enabled_optimizes_posted_graph_no_training():
    client = TestClient(create_app(enabled=True))
    resp = client.post(
        "/v1/optimize-route",
        json={
            "start_node": "a",
            "horizon": 4,
            "train_steps": 0,
            "node_rows": [
                {"node_id": "a", "lat": 10.0, "lon": 20.0, "vessel_count": 5},
                {"node_id": "b", "lat": 10.1, "lon": 20.0, "vessel_count": 3},
            ],
            "edge_rows": [{"from_node": "a", "to_node": "b", "frequency": 2}],
        },
    )
    assert resp.status_code == 200
    assert resp.json()["graph"] == "posted"


def test_unknown_start_node_is_422():
    client = TestClient(create_app(enabled=True))
    resp = client.post(
        "/v1/optimize-route", json={"start_node": "does-not-exist", "train_steps": 0}
    )
    assert resp.status_code == 422


def test_node_rows_without_edge_rows_is_422():
    client = TestClient(create_app(enabled=True))
    resp = client.post(
        "/v1/optimize-route",
        json={
            "start_node": "a",
            "train_steps": 0,
            "node_rows": [{"node_id": "a", "lat": 1.0, "lon": 2.0, "vessel_count": 1}],
        },
    )
    assert resp.status_code == 422


def test_invalid_posted_graph_is_422():
    client = TestClient(create_app(enabled=True))
    resp = client.post(
        "/v1/optimize-route",
        json={
            "start_node": "a",
            "train_steps": 0,
            "node_rows": [{"node_id": "a", "lat": 1.0, "lon": 2.0, "vessel_count": 1}],
            "edge_rows": [{"from_node": "a", "to_node": "a", "frequency": 1}],  # self-loop
        },
    )
    assert resp.status_code == 422


@pytest.mark.parametrize("enabled", [True, False])
def test_app_factory_default_reads_settings(enabled, monkeypatch):
    # create_app() with no explicit flag must honor the settings toggle
    from app.config import Settings, get_settings

    get_settings.cache_clear()
    monkeypatch.setenv("HM_ENABLE_PHASE5_PPO_STRETCH", "true" if enabled else "false")
    assert Settings().enable_phase5_ppo_stretch is enabled
    client = TestClient(create_app())
    assert client.get("/healthz").json()["enabled"] is enabled
    get_settings.cache_clear()
