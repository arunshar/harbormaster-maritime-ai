from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from lake.backfill.transforms import (
    canonicalize_positions,
    cluster_waypoints,
    derive_corridor_graph,
    derive_edges,
    rdp_simplify,
    simplify_track,
)

EXPECTATIONS = Path(__file__).parent.parent / "fixtures" / "expectations.json"


def test_canonicalize_positions_types_dedup_and_sorts():
    df = pd.DataFrame(
        [
            {
                "mmsi": "1",
                "t": "2024-06-01T00:01:00Z",
                "lat": 40.1,
                "lon": -74.1,
                "sog": 5.0,
                "cog": 10.0,
            },
            {
                "mmsi": "1",
                "t": "2024-06-01T00:00:00Z",
                "lat": 40.0,
                "lon": -74.0,
                "sog": 5.0,
                "cog": 10.0,
            },
            # exact duplicate of the row above: must collapse to one
            {
                "mmsi": "1",
                "t": "2024-06-01T00:00:00Z",
                "lat": 40.0,
                "lon": -74.0,
                "sog": 5.0,
                "cog": 10.0,
            },
        ]
    )
    out = canonicalize_positions(df)
    assert len(out) == 2
    assert out["mmsi"].dtype == "int64"
    assert list(out["t"]) == sorted(out["t"])
    assert out.iloc[0]["t"] < out.iloc[1]["t"]


def test_rdp_simplify_straight_line_collapses_to_endpoints():
    points = [(0.0, 0.0), (1.0, 0.0), (2.0, 0.0), (3.0, 0.0), (4.0, 0.0)]
    result = rdp_simplify(points, epsilon_m=0.5)
    assert result == [(0.0, 0.0), (4.0, 0.0)]


def test_rdp_simplify_keeps_a_real_turn():
    # an L-shaped path: (2,0) sits far off the (0,0)-(2,2) chord
    points = [(0.0, 0.0), (1.0, 0.0), (2.0, 0.0), (2.0, 1.0), (2.0, 2.0)]
    result = rdp_simplify(points, epsilon_m=0.5)
    assert result == [(0.0, 0.0), (2.0, 0.0), (2.0, 2.0)]


def test_rdp_simplify_short_input_returned_as_is():
    assert rdp_simplify([(0.0, 0.0)], epsilon_m=1.0) == [(0.0, 0.0)]
    assert rdp_simplify([(0.0, 0.0), (1.0, 1.0)], epsilon_m=1.0) == [(0.0, 0.0), (1.0, 1.0)]


def _track(mmsi: int, latlons: list[tuple[float, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "mmsi": mmsi,
            "t": pd.date_range("2024-06-01", periods=len(latlons), freq="min", tz="UTC"),
            "lat": [p[0] for p in latlons],
            "lon": [p[1] for p in latlons],
            "sog": 10.0,
            "cog": 0.0,
        }
    )


def test_simplify_track_keeps_start_turn_and_end_for_a_real_zigzag():
    # ~5.5km northward run, then a turn, then ~5.5km eastward run: the turn is
    # well above a 200m epsilon, the straight runs are not.
    track = _track(
        1,
        [
            (40.30, -74.15),
            (40.32, -74.15),
            (40.35, -74.15),
            (40.35, -74.10),
            (40.35, -74.05),
        ],
    )
    simplified = simplify_track(track, epsilon_m=200.0)
    assert len(simplified) == 3
    assert simplified.iloc[0]["lat"] == 40.30
    assert simplified.iloc[1]["lat"] == 40.35 and simplified.iloc[1]["lon"] == -74.15
    assert simplified.iloc[-1]["lon"] == -74.05


def test_simplify_track_short_track_returned_as_is():
    track = _track(1, [(40.0, -74.0), (40.1, -74.1)])
    out = simplify_track(track, epsilon_m=200.0)
    assert len(out) == 2


def _synthetic_multivessel_zigzag(n_vessels: int) -> pd.DataFrame:
    """n_vessels tracks, each turning near the same two waypoints (jittered a
    few tens of meters, tight vs. a 200m epsilon and the clustering radius),
    with start/end points scattered many tens of km apart so each is an
    isolated singleton (HDBSCAN noise), not a third/fourth shared cluster."""
    waypoint_a = (40.35, -74.15)
    waypoint_b = (40.35, -74.05)
    frames = []
    for i in range(n_vessels):
        jitter = 0.0002 * i  # ~20m per vessel, small vs. the ~9km leg lengths
        spread = 0.5 * i  # ~55km per vessel: starts/ends never cluster with each other
        start = (40.30 - spread, -74.20 - spread)
        end = (40.35 + spread, -74.00 + spread)
        a = (waypoint_a[0] + jitter, waypoint_a[1])
        b = (waypoint_b[0], waypoint_b[1] + jitter)
        frames.append(_track(100 + i, [start, a, b, end]))
    return pd.concat(frames, ignore_index=True)


def test_cluster_waypoints_and_derive_edges_find_the_shared_corridor():
    df = _synthetic_multivessel_zigzag(n_vessels=3)
    simplified = pd.concat(
        [simplify_track(g, epsilon_m=200.0) for _, g in df.groupby("mmsi", sort=False)],
        ignore_index=True,
    )
    # min_cluster_size=3: with exactly 3 vessels, no pair of far-flung singleton
    # starts/ends (each ~55km+ from every other) can ever reach the size-3
    # floor on its own, so only the two genuinely 3-vessel-shared waypoints
    # can possibly qualify; this holds regardless of HDBSCAN's exact stability
    # computation, not just for this fixture's specific coordinates.
    nodes = cluster_waypoints(simplified, min_cluster_size=3)

    # 3 vessels each contribute a start, waypoint A, waypoint B, and an end;
    # only A and B are shared across all 3 vessels, so exactly 2 nodes should
    # survive HDBSCAN (the unshared starts/ends are noise).
    assert len(nodes) == 2
    assert set(nodes["vessel_count"]) == {3}

    edges = derive_edges(simplified, nodes)
    assert len(edges) == 1
    assert edges.iloc[0]["frequency"] == 3


def test_derive_edges_empty_when_no_nodes():
    empty_nodes = pd.DataFrame(columns=["node_id", "lat", "lon", "vessel_count"])
    simplified = pd.DataFrame(columns=["mmsi", "t", "lat", "lon", "sog", "cog"])
    edges = derive_edges(simplified, empty_nodes)
    assert edges.empty


def test_cluster_waypoints_empty_input_returns_empty_nodes():
    empty = pd.DataFrame(columns=["mmsi", "t", "lat", "lon", "sog", "cog"])
    nodes = cluster_waypoints(empty)
    assert nodes.empty


def test_derive_corridor_graph_end_to_end_pinned_counts():
    df = _synthetic_multivessel_zigzag(n_vessels=4)
    nodes, edges = derive_corridor_graph(df, epsilon_m=200.0, min_cluster_size=3)
    assert len(nodes) == 2
    assert len(edges) == 1
    assert edges.iloc[0]["frequency"] == 4
    assert set(nodes["vessel_count"]) == {4}


def test_corridor_graph_counts_match_the_pinned_expectation():
    pinned = json.loads(EXPECTATIONS.read_text())["corridor_graph_synthetic_fixture"]

    df3 = _synthetic_multivessel_zigzag(n_vessels=3)
    nodes3, edges3 = derive_corridor_graph(df3, epsilon_m=200.0, min_cluster_size=3)
    assert len(nodes3) == pinned["n_vessels_3"]["nodes"]
    assert len(edges3) == pinned["n_vessels_3"]["edges"]
    assert edges3.iloc[0]["frequency"] == pinned["n_vessels_3"]["edge_frequency"]

    df4 = _synthetic_multivessel_zigzag(n_vessels=4)
    nodes4, edges4 = derive_corridor_graph(df4, epsilon_m=200.0, min_cluster_size=3)
    assert len(nodes4) == pinned["n_vessels_4"]["nodes"]
    assert len(edges4) == pinned["n_vessels_4"]["edges"]
    assert edges4.iloc[0]["frequency"] == pinned["n_vessels_4"]["edge_frequency"]
