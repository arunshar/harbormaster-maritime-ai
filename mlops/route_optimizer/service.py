"""The PPO route-optimizer's own FastAPI microservice (gate 5.7).

Structurally isolated, on purpose:
- It is behind ``enable_phase5_ppo_stretch`` (default false). Disabled, every
  optimize call returns HTTP 503; the service advertises its own state at
  ``/healthz`` and never pretends to be part of the platform's front door.
- It imports NOTHING from ``serving/app/orchestrator.py`` or ``mlops/promote.py``
  and neither imports it back; ``test_route_optimizer_service.py`` pins that by
  scanning the real import graph, so the stretch can never leak into the core
  scoring or promotion path (acceptance criterion g).
- It never runs on AWS GPU (a standing platform decision): the corridor policy
  is a tiny CPU tabular model; a real request rebuilds the graph from posted
  corridor rows and returns the greedy route under a freshly trained policy.

This is deployed, if ever, as a separate container in a demo window, not wired
into the API Gateway front door.
"""

from __future__ import annotations

from typing import Annotated, Literal

from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from app.config import get_settings
from mlops.route_optimizer.feasibility import v_max_mps_for
from mlops.route_optimizer.graph import CorridorGraph, tiny_synthetic_graph
from mlops.route_optimizer.ppo import PpoConfig, TabularPolicy
from mlops.route_optimizer.rollout import greedy_route, train_optimizer

__version__ = "0.1.0"


def _resolve_enabled(request: Request) -> bool:
    """The toggle state for this request: a test-forced override on app.state if
    set, else the ``enable_phase5_ppo_stretch`` setting. Module-level (not a
    closure) so FastAPI can resolve it under ``from __future__ import
    annotations``; ``Annotated``, not a ``Depends`` default, to satisfy B008."""
    forced = getattr(request.app.state, "forced_enabled", None)
    if forced is not None:
        return bool(forced)
    return get_settings().enable_phase5_ppo_stretch


EnabledDep = Annotated[bool, Depends(_resolve_enabled)]


class OptimizeRequest(BaseModel):
    start_node: str = Field(..., description="node_id the route starts from")
    horizon: int = Field(8, ge=1, le=64, description="max hops in the returned walk")
    domain: Literal["vessel", "vehicle", "pedestrian", "uav"] = Field(
        "vessel", description="speed model for the S-KBM feasibility gate"
    )
    coverage_weight: float = Field(1.0, ge=0.0)
    fuel_weight: float = Field(1.0, ge=0.0)
    train_steps: int = Field(40, ge=0, le=500, description="CPU PPO steps before the read-out")
    n_rollouts: int = Field(24, ge=1, le=256)
    seed: int = Field(0, ge=0)
    node_rows: list[dict] | None = Field(
        None, description="corridor_graph_nodes rows; omitted -> the tiny synthetic demo graph"
    )
    edge_rows: list[dict] | None = None


class OptimizeResponse(BaseModel):
    route: list[str]
    reward: float
    n_hops: int
    trained_steps: int
    graph: str  # "posted" | "synthetic-demo"


class HealthOut(BaseModel):
    status: str
    service: str = "harbormaster-route-optimizer"
    enabled: bool


def _build_graph(req: OptimizeRequest) -> tuple[CorridorGraph, str]:
    if req.node_rows is None and req.edge_rows is None:
        return tiny_synthetic_graph(), "synthetic-demo"
    if req.node_rows is None or req.edge_rows is None:
        raise HTTPException(422, "node_rows and edge_rows must be provided together")
    try:
        return CorridorGraph.from_rows(req.node_rows, req.edge_rows), "posted"
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


def create_app(*, enabled: bool | None = None) -> FastAPI:
    """Build the service. ``enabled`` defaults to the settings toggle; pass it
    explicitly in tests to exercise both the on and off paths without touching
    the process environment or the ``get_settings`` cache."""
    app = FastAPI(title="Harbormaster-RouteOptimizer", version=__version__)
    # None -> read the setting per request; True/False -> a test/demo override.
    app.state.forced_enabled = enabled

    @app.get("/healthz", response_model=HealthOut)
    async def healthz(is_on: EnabledDep) -> HealthOut:
        return HealthOut(status="ok", enabled=is_on)

    @app.post("/v1/optimize-route", response_model=OptimizeResponse)
    def optimize(req: OptimizeRequest, is_on: EnabledDep) -> OptimizeResponse:
        if not is_on:
            raise HTTPException(
                503, "PPO route-optimizer stretch is disabled (enable_phase5_ppo_stretch=false)"
            )
        graph, graph_kind = _build_graph(req)
        try:
            start_idx = graph.index_of(req.start_node)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

        # total_steps spans the actual run so the cosine LR schedule decays over
        # exactly train_steps (a fixed PpoConfig() total_steps would let the
        # schedule turn back UP once train_steps exceeded it).
        cfg = PpoConfig(total_steps=max(req.train_steps, 1))
        # Train a fresh CPU policy per request and read it out greedily. A
        # persistent, continually-trained policy is out of scope for a labeled
        # stretch (PHASE_5.md); train_steps=0 reads out the untrained policy,
        # which is the feasibility-respecting uniform-then-argmax heuristic. The
        # reward weights are threaded into training so the policy optimizes the
        # same reward the greedy read-out below scores.
        if req.train_steps > 0:
            policy, _ = train_optimizer(
                graph,
                cfg,
                seed=req.seed,
                n_steps=req.train_steps,
                n_rollouts=req.n_rollouts,
                horizon=req.horizon,
                start_idx=start_idx,
                domain=req.domain,
                coverage_weight=req.coverage_weight,
                fuel_weight=req.fuel_weight,
            )
        else:
            policy = TabularPolicy(graph.n_nodes, graph.max_out_degree)
        greedy = greedy_route(
            policy,
            graph,
            start_idx=start_idx,
            horizon=req.horizon,
            dt_s=cfg.hop_seconds,
            v_max_mps=v_max_mps_for(req.domain),
            coverage_weight=req.coverage_weight,
            fuel_weight=req.fuel_weight,
        )
        return OptimizeResponse(
            route=greedy.node_ids,
            reward=greedy.reward,
            n_hops=max(0, len(greedy.node_ids) - 1),
            trained_steps=req.train_steps,
            graph=graph_kind,
        )

    return app


app = create_app()
