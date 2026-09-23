"""Gate 5.7: CorridorGraph strict validation + tiny_synthetic_graph invariants."""

from __future__ import annotations

import numpy as np
import pytest

from mlops.route_optimizer.graph import CorridorGraph, tiny_synthetic_graph

_NODES = [
    {"node_id": "a", "lat": 10.0, "lon": 20.0, "vessel_count": 5},
    {"node_id": "b", "lat": 10.1, "lon": 20.0, "vessel_count": 3},
]
_EDGES = [{"from_node": "a", "to_node": "b", "frequency": 2}]


def test_from_rows_builds_index_view():
    g = CorridorGraph.from_rows(_NODES, _EDGES)
    assert g.n_nodes == 2
    assert g.node_ids == ("a", "b")
    assert g.total_vessel_count == 8
    assert g.neighbors == ((1,), ())
    assert g.max_out_degree == 1
    assert g.index_of("b") == 1
    assert g.edge_frequency == ((2,), ())
    assert isinstance(g.lat, np.ndarray) and g.lat.dtype == np.float64


def test_empty_nodes_rejected():
    with pytest.raises(ValueError, match="no action space"):
        CorridorGraph.from_rows([], _EDGES)


def test_missing_node_field_rejected():
    bad = [{"node_id": "a", "lat": 1.0, "lon": 2.0}]  # no vessel_count
    with pytest.raises(ValueError, match="missing"):
        CorridorGraph.from_rows(bad, [])


def test_duplicate_node_rejected():
    dup = _NODES + [{"node_id": "a", "lat": 0.0, "lon": 0.0, "vessel_count": 1}]
    with pytest.raises(ValueError, match="duplicate node_id"):
        CorridorGraph.from_rows(dup, [])


@pytest.mark.parametrize("lat,lon", [(91.0, 0.0), (-91.0, 0.0), (0.0, 181.0), (0.0, -181.0)])
def test_out_of_range_latlon_rejected(lat, lon):
    bad = [{"node_id": "a", "lat": lat, "lon": lon, "vessel_count": 1}]
    with pytest.raises(ValueError, match="out of range"):
        CorridorGraph.from_rows(bad, [])


def test_negative_vessel_count_rejected():
    bad = [{"node_id": "a", "lat": 1.0, "lon": 2.0, "vessel_count": -1}]
    with pytest.raises(ValueError, match="negative"):
        CorridorGraph.from_rows(bad, [])


def test_missing_edge_field_rejected():
    with pytest.raises(ValueError, match="missing"):
        CorridorGraph.from_rows(_NODES, [{"from_node": "a", "to_node": "b"}])


def test_edge_unknown_node_rejected():
    with pytest.raises(ValueError, match="unknown node"):
        CorridorGraph.from_rows(_NODES, [{"from_node": "a", "to_node": "z", "frequency": 1}])


def test_self_loop_rejected():
    with pytest.raises(ValueError, match="self-loop"):
        CorridorGraph.from_rows(_NODES, [{"from_node": "a", "to_node": "a", "frequency": 1}])


def test_duplicate_edge_rejected():
    dup = _EDGES + [{"from_node": "a", "to_node": "b", "frequency": 9}]
    with pytest.raises(ValueError, match="duplicate edge"):
        CorridorGraph.from_rows(_NODES, dup)


def test_frequency_below_one_rejected():
    with pytest.raises(ValueError, match="frequency must be"):
        CorridorGraph.from_rows(_NODES, [{"from_node": "a", "to_node": "b", "frequency": 0}])


def test_index_of_unknown_raises():
    g = CorridorGraph.from_rows(_NODES, _EDGES)
    with pytest.raises(ValueError, match="unknown node_id"):
        g.index_of("zzz")


def test_action_slot_non_edge_raises():
    g = CorridorGraph.from_rows(_NODES, _EDGES)
    # b has no outgoing edge to a
    with pytest.raises(ValueError, match="the action space is the edge set"):
        g.action_slot(1, 0)


def test_action_slot_edge_resolves():
    g = CorridorGraph.from_rows(_NODES, _EDGES)
    assert g.action_slot(0, 1) == 0


def test_tiny_synthetic_graph_shape():
    g = tiny_synthetic_graph()
    assert g.n_nodes == 4
    assert g.total_vessel_count == 18
    assert g.max_out_degree == 2
    assert g.neighbors == ((1, 2), (3, 0), (0,), (2,))
    # every edge distance is a positive great-circle length
    for row in g.edge_distance_m:
        for d in row:
            assert d > 0.0
