"""S-KBM kinematic feasibility filter for PPO routes (gate 5.7), REUSING the
existing gate, not a new one.

``serving/app/agents/validator.py`` enforces one hard invariant on real
trajectories: two anchors are mutually reachable only when the great-circle
distance between them is coverable under ``v_max`` in the elapsed time, i.e.
``dist(A, B) <= v_max * (t_B - t_A)`` with a 1.05 slack (validator.py:48). The
PPO action space is the corridor edge set, so the SAME condition is the only
feasibility rule the optimizer needs: a hop ``i -> j`` is feasible when the
vessel can steam ``edge_distance_m[i][slot]`` within the per-hop time budget
under the same speed model.

This module reuses ``speed_bounds_for`` (the real speed model, imported, not
re-derived) and the same 1.05 tolerance constant, and expresses the same
inequality. It deliberately does NOT re-enter ``ValidatorAgent.validate``: that
path is async and typed over ``RendezvousRegion`` / ``Anchor`` pydantic models,
a different shape than corridor edges, so calling it here would be a forced
adapter, not reuse. The reused thing is the kinematic law and its speed model,
applied to the edge geometry ``graph.py`` already computed with the same
``haversine_m``.
"""

from __future__ import annotations

import numpy as np

from app.components.space_time_prism import speed_bounds_for
from mlops.route_optimizer.graph import CorridorGraph

__all__ = [
    "KINEMATIC_TOLERANCE",
    "feasible_hop",
    "feasible_action_mask",
    "v_max_mps_for",
]

# Verbatim from ValidatorAgent's ``v_req > bounds.v_max_mps * 1.05`` gate
# (serving/app/agents/validator.py:48). Kept as a named constant here so the two
# gates cannot silently drift; a change to one must be a deliberate change to
# both.
KINEMATIC_TOLERANCE = 1.05


def v_max_mps_for(
    domain: str = "vessel",
    *,
    vessel_v_max_kts: float = 25.0,
    vehicle_v_max_kmh: float = 130.0,
) -> float:
    """The max speed in m/s from the real speed model (``speed_bounds_for``).

    Defaults mirror ``app.config.Settings`` (vessel 25 kts, vehicle 130 km/h),
    so the optimizer's feasibility gate and the serving plane's validator agree
    on the physics by construction.
    """
    return speed_bounds_for(
        domain,
        vessel_v_max_kts=vessel_v_max_kts,
        vehicle_v_max_kmh=vehicle_v_max_kmh,
    ).v_max_mps


def feasible_hop(
    distance_m: float,
    dt_s: float,
    v_max_mps: float,
    *,
    tol: float = KINEMATIC_TOLERANCE,
) -> bool:
    """The single reachability rule: ``dist <= v_max * dt * tol``, ``dt > 0``.

    A non-positive time budget is infeasible (a corridor hop takes real time);
    this mirrors the validator skipping ``dt_s <= 0`` anchor pairs rather than
    dividing by zero.
    """
    if dt_s <= 0.0 or v_max_mps <= 0.0:
        return False
    return bool(distance_m <= v_max_mps * dt_s * tol)


def feasible_action_mask(
    graph: CorridorGraph,
    node_idx: int,
    *,
    dt_s: float,
    v_max_mps: float,
    tol: float = KINEMATIC_TOLERANCE,
) -> np.ndarray:
    """Boolean mask over node ``node_idx``'s action slots: True where the hop
    is kinematically reachable under the shared gate.

    Length equals the node's out-degree (its number of corridor edges, the
    action slots ``CorridorGraph`` exposes). An all-False mask means every
    outgoing corridor is infeasible in the given per-hop budget: the rollout
    treats that as a terminal state rather than forcing a teleport.
    """
    distances = graph.edge_distance_m[node_idx]
    return np.array(
        [feasible_hop(d, dt_s, v_max_mps, tol=tol) for d in distances],
        dtype=bool,
    )
