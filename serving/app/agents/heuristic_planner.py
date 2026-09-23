"""HeuristicPlanner. Deterministic plan routing by history length. No LLM.

Replaces the GeoTrace PlannerAgent for the live AIS-scoring path: instead of an
LLM emitting a PlanGraph, this routes by how much track history the vessel has,
so the live path costs zero tokens.

  history 0  (only the current fix)  -> corridor check only
  history 1  (one prior fix)         -> + prism, speed, kinematic validate
  history 3+ (enough for a window)   -> + abnormal-gap detection (STAGD + AGM)

RendezvousFinder / TGARD is not routed here: it needs two distinct vessel
trajectories and activates in the multi-vessel endpoint, not single-vessel scoring.
"""

from __future__ import annotations

from app.models import PlanGraph, PlanNode, PlanNodeKind


class HeuristicPlanner:
    def plan(self, n_history: int) -> PlanGraph:
        """Build the deterministic plan for a vessel with `n_history` prior fixes."""

        nodes: list[PlanNode] = [
            PlanNode(id="corridor", kind=PlanNodeKind.CORRIDOR, rationale="single-fix lane check"),
        ]
        if n_history >= 1:
            nodes.append(PlanNode(id="prism", kind=PlanNodeKind.PRISM, rationale="latest segment"))
            nodes.append(PlanNode(id="speed", kind=PlanNodeKind.SPEED, rationale="latest segment"))
            nodes.append(
                PlanNode(
                    id="validate",
                    kind=PlanNodeKind.VALIDATE,
                    deps=("prism",),
                    rationale="S-KBM region gate",
                )
            )
        if n_history >= 3:
            nodes.append(PlanNode(id="gaps", kind=PlanNodeKind.GAPS, rationale="STAGD + AGM"))

        rationale = f"deterministic route for n_history={n_history}"
        return PlanGraph(nodes=tuple(nodes), rationale=rationale)
