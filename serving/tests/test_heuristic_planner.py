"""HeuristicPlanner routing tests (Phase 1.2)."""

from __future__ import annotations

from app.agents.heuristic_planner import HeuristicPlanner
from app.models import PlanNodeKind

K = PlanNodeKind


def _kinds(n_history: int) -> set[PlanNodeKind]:
    return {n.kind for n in HeuristicPlanner().plan(n_history).nodes}


def test_history_0_routes_corridor_only():
    assert _kinds(0) == {K.CORRIDOR}


def test_history_1_adds_prism_speed_validate():
    assert _kinds(1) == {K.CORRIDOR, K.PRISM, K.SPEED, K.VALIDATE}


def test_history_3plus_adds_gaps():
    assert _kinds(3) == {K.CORRIDOR, K.PRISM, K.SPEED, K.VALIDATE, K.GAPS}
    assert _kinds(50) == _kinds(3)


def test_validate_depends_on_prism_in_topo_order():
    layers = HeuristicPlanner().plan(3).topo_layers()
    seen: set[str] = set()
    for layer in layers:
        for node in layer:
            assert all(d in seen for d in node.deps)
            if node.kind is K.VALIDATE:
                assert "prism" in node.deps
        seen |= {n.id for n in layer}
