"""Corridor-graph action space for the PPO stretch service (gate 5.7).

The graph IS the Phase 3 lake's ``corridor_graph_nodes`` /
``corridor_graph_edges`` Iceberg tables (``lake/iceberg.py``):
``CorridorGraph.from_rows`` takes rows in exactly those schemas (node_id /
lat / lon / vessel_count and from_node / to_node / frequency) and nothing
else, so a real Iceberg scan's ``to_pylist()`` drops straight in. This
module only computes, it never fetches (the injected-I/O convention
``mlops/drift.py`` and ``mlops/tenant_drift.py`` state): reading the real
tables is the caller's job, and ``tiny_synthetic_graph()`` is the zero-AWS,
zero-GPU stand-in the smoke and the unit tests run on.

Distances are ``haversine_m`` from the S-KBM prism component itself, so the
reward's fuel term and the feasibility gate measure the same geometry.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from app.components.space_time_prism import haversine_m

__all__ = ["CorridorGraph", "tiny_synthetic_graph"]

_NODE_FIELDS = ("node_id", "lat", "lon", "vessel_count")
_EDGE_FIELDS = ("from_node", "to_node", "frequency")


@dataclass(frozen=True)
class CorridorGraph:
    """An immutable, index-based view of the corridor tables.

    ``neighbors[i]`` are the out-edge target node indices of node ``i`` (the
    action slots, in edge-row order); ``edge_distance_m[i]`` the matching
    great-circle lengths; ``edge_frequency[i]`` the matching corridor
    frequencies. ``max_out_degree`` sizes the tabular policy's action axis.
    """

    node_ids: tuple[str, ...]
    lat: np.ndarray
    lon: np.ndarray
    vessel_count: np.ndarray
    neighbors: tuple[tuple[int, ...], ...]
    edge_distance_m: tuple[tuple[float, ...], ...]
    edge_frequency: tuple[tuple[int, ...], ...]

    @property
    def n_nodes(self) -> int:
        return len(self.node_ids)

    @property
    def max_out_degree(self) -> int:
        return max((len(n) for n in self.neighbors), default=0)

    @property
    def total_vessel_count(self) -> int:
        return int(self.vessel_count.sum())

    def index_of(self, node_id: str) -> int:
        try:
            return self.node_ids.index(node_id)
        except ValueError:
            raise ValueError(f"unknown node_id {node_id!r}") from None

    def action_slot(self, from_idx: int, to_idx: int) -> int:
        """The action-slot index of edge from_idx -> to_idx, or ValueError."""
        try:
            return self.neighbors[from_idx].index(to_idx)
        except ValueError:
            raise ValueError(
                f"no corridor edge {self.node_ids[from_idx]!r} -> "
                f"{self.node_ids[to_idx]!r}; the action space is the edge set"
            ) from None

    @classmethod
    def from_rows(
        cls,
        node_rows: list[dict[str, Any]],
        edge_rows: list[dict[str, Any]],
    ) -> CorridorGraph:
        """Build from rows in the exact Iceberg table schemas, strictly validated."""
        if not node_rows:
            raise ValueError("corridor_graph_nodes is empty; no action space")
        ids: list[str] = []
        lat: list[float] = []
        lon: list[float] = []
        vessel_count: list[int] = []
        for row in node_rows:
            missing = [f for f in _NODE_FIELDS if f not in row]
            if missing:
                raise ValueError(f"node row missing {missing}; schema is {_NODE_FIELDS}")
            node_id = str(row["node_id"])
            if node_id in ids:
                raise ValueError(f"duplicate node_id {node_id!r}")
            la, lo = float(row["lat"]), float(row["lon"])
            if not (-90.0 <= la <= 90.0 and -180.0 <= lo <= 180.0):
                raise ValueError(f"node {node_id!r} lat/lon out of range: ({la}, {lo})")
            vc = int(row["vessel_count"])
            if vc < 0:
                raise ValueError(f"node {node_id!r} vessel_count is negative: {vc}")
            ids.append(node_id)
            lat.append(la)
            lon.append(lo)
            vessel_count.append(vc)

        index = {node_id: i for i, node_id in enumerate(ids)}
        adj: list[list[int]] = [[] for _ in ids]
        dist: list[list[float]] = [[] for _ in ids]
        freq: list[list[int]] = [[] for _ in ids]
        seen_edges: set[tuple[str, str]] = set()
        for row in edge_rows:
            missing = [f for f in _EDGE_FIELDS if f not in row]
            if missing:
                raise ValueError(f"edge row missing {missing}; schema is {_EDGE_FIELDS}")
            a, b = str(row["from_node"]), str(row["to_node"])
            if a not in index or b not in index:
                raise ValueError(f"edge ({a!r}, {b!r}) references an unknown node")
            if a == b:
                raise ValueError(f"self-loop edge on {a!r}; a corridor to itself is degenerate")
            if (a, b) in seen_edges:
                raise ValueError(f"duplicate edge ({a!r}, {b!r})")
            f = int(row["frequency"])
            if f < 1:
                raise ValueError(f"edge ({a!r}, {b!r}) frequency must be >= 1, got {f}")
            seen_edges.add((a, b))
            i, j = index[a], index[b]
            adj[i].append(j)
            dist[i].append(haversine_m(lat[i], lon[i], lat[j], lon[j]))
            freq[i].append(f)

        return cls(
            node_ids=tuple(ids),
            lat=np.asarray(lat, dtype=np.float64),
            lon=np.asarray(lon, dtype=np.float64),
            vessel_count=np.asarray(vessel_count, dtype=np.int64),
            neighbors=tuple(tuple(n) for n in adj),
            edge_distance_m=tuple(tuple(d) for d in dist),
            edge_frequency=tuple(tuple(f) for f in freq),
        )


def tiny_synthetic_graph() -> CorridorGraph:
    """The fixed 4-node, 6-edge fixture the smoke and the pinned checksum use.

    A ~5.6 x ~9.9 km box of open-water waypoints. Changing ANY value here
    invalidates ``ppo_stretch_expectations`` in
    ``mlops/fixtures/expectations.json``; update both in the same commit.
    """
    nodes = [
        {"node_id": "n0", "lat": 10.00, "lon": 20.00, "vessel_count": 8},
        {"node_id": "n1", "lat": 10.05, "lon": 20.00, "vessel_count": 3},
        {"node_id": "n2", "lat": 10.00, "lon": 20.09, "vessel_count": 5},
        {"node_id": "n3", "lat": 10.05, "lon": 20.09, "vessel_count": 2},
    ]
    edges = [
        {"from_node": "n0", "to_node": "n1", "frequency": 4},
        {"from_node": "n1", "to_node": "n3", "frequency": 2},
        {"from_node": "n3", "to_node": "n2", "frequency": 3},
        {"from_node": "n2", "to_node": "n0", "frequency": 5},
        {"from_node": "n0", "to_node": "n2", "frequency": 1},
        {"from_node": "n1", "to_node": "n0", "frequency": 2},
    ]
    return CorridorGraph.from_rows(nodes, edges)
