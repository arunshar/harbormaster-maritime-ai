"""``coverage_minus_fuel``: the gate 5.7 reward over the corridor graph.

Pure and deterministic (float64 arithmetic only), because its value on the
canonical tiny-fixture route is checksummed in
``mlops/fixtures/expectations.json`` under ``ppo_stretch_expectations``.

Definition:
- coverage: vessel_count of the DISTINCT visited nodes over the graph's
  total vessel_count (revisits earn nothing twice), in [0, 1].
- fuel: great-circle meters actually steamed along the route's edges,
  normalized by ``FUEL_NORM_M`` (100 km of steaming costs ``fuel_weight``).
- reward = coverage_weight * coverage - fuel_weight * fuel.

A route must be a walk on the corridor edge set (the action space per
docs/phases/PHASE_5.md gate 5.7); a hop with no corridor edge is a
ValueError, not a low reward, so the trainer can never learn to teleport.
"""

from __future__ import annotations

from collections.abc import Sequence

from mlops.route_optimizer.graph import CorridorGraph

__all__ = ["FUEL_NORM_M", "coverage_minus_fuel"]

FUEL_NORM_M = 100_000.0


def coverage_minus_fuel(
    route: Sequence[str],
    corridor_graph: CorridorGraph,
    *,
    coverage_weight: float = 1.0,
    fuel_weight: float = 1.0,
) -> float:
    """Reward of a node-id route over the corridor graph. Empty route -> 0.0."""
    if not route:
        return 0.0
    idxs = [corridor_graph.index_of(n) for n in route]
    total = corridor_graph.total_vessel_count
    covered = sum(int(corridor_graph.vessel_count[i]) for i in sorted(set(idxs)))
    coverage = (covered / total) if total > 0 else 0.0

    dist_m = 0.0
    for a, b in zip(idxs, idxs[1:], strict=False):
        slot = corridor_graph.action_slot(a, b)  # ValueError on a non-edge hop
        dist_m += corridor_graph.edge_distance_m[a][slot]

    return coverage_weight * coverage - fuel_weight * (dist_m / FUEL_NORM_M)
