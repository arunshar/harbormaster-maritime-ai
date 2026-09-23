"""Pure transform functions for the Phase 3 MarineCadastre backfill (gate 3.2).

Everything here is plain pandas/NumPy/scikit-learn, zero Spark and zero AWS,
so it is fully unit-testable without a JVM. `lake/backfill/job.py` is a thin PySpark wrapper that
calls these same functions per-partition/per-group on EMR; the functions
themselves do not know they are ever run inside Spark.

The corridor-graph derivation (RDP simplification + HDBSCAN clustering) is a
clean, personal implementation over public MarineCadastre-shaped data (see
docs/HONESTY.md). It produces the same lane-and-waypoint shape as the
hand-placed Phase 1 demo artifact in serving/app/artifacts/corridors.json,
rebuilt here for a distributed multi-region backfill.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

REQUIRED_RAW_COLUMNS: tuple[str, ...] = ("mmsi", "t", "lat", "lon", "sog", "cog")

EARTH_RADIUS_M = 6_371_000.0


def canonicalize_positions(df: pd.DataFrame) -> pd.DataFrame:
    """Raw extract -> the ais_history shape: typed, deduped, sorted.

    mmsi is coerced to int, t to a UTC timestamp; exact-duplicate (mmsi, t)
    rows collapse to one (a common MarineCadastre artifact from overlapping
    receiver coverage); rows sort by (mmsi, t) so downstream per-vessel
    processing (RDP, diffs) sees a well-ordered track.
    """
    out = df.loc[:, list(REQUIRED_RAW_COLUMNS)].copy()
    out["mmsi"] = out["mmsi"].astype("int64")
    out["t"] = pd.to_datetime(out["t"], utc=True, format="ISO8601")
    out = out.drop_duplicates(subset=["mmsi", "t"], keep="first")
    out = out.sort_values(["mmsi", "t"]).reset_index(drop=True)
    return out


def _equirectangular_xy(
    lat: np.ndarray, lon: np.ndarray, *, lat0: float, lon0: float
) -> tuple[np.ndarray, np.ndarray]:
    """Project to local meters around (lat0, lon0). Euclidean to first order
    over a demo/regional extent; matches the projection note in
    docs/corridor-detector.md (switch to a real UTM zone for larger regions,
    which is out of scope here)."""
    lat0_rad = math.radians(lat0)
    x = EARTH_RADIUS_M * np.radians(lon - lon0) * math.cos(lat0_rad)
    y = EARTH_RADIUS_M * np.radians(lat - lat0)
    return x, y


def _perpendicular_distance(
    point: tuple[float, float], start: tuple[float, float], end: tuple[float, float]
) -> float:
    x0, y0 = point
    x1, y1 = start
    x2, y2 = end
    if (x1, y1) == (x2, y2):
        return math.hypot(x0 - x1, y0 - y1)
    num = abs((y2 - y1) * x0 - (x2 - x1) * y0 + x2 * y1 - y2 * x1)
    den = math.hypot(y2 - y1, x2 - x1)
    return num / den


def rdp_simplify(points: list[tuple[float, float]], epsilon_m: float) -> list[tuple[float, float]]:
    """Ramer-Douglas-Peucker line simplification, from scratch (no library):
    keep the two endpoints plus the point farthest from the chord whenever
    that distance exceeds epsilon_m, recursing on both halves. A vessel's
    simplified track is its RDP-retained points, which double as candidate
    waypoints (turns / course changes) for the corridor graph."""
    if len(points) < 3:
        return list(points)

    start, end = points[0], points[-1]
    d_max = 0.0
    idx = 0
    for i in range(1, len(points) - 1):
        d = _perpendicular_distance(points[i], start, end)
        if d > d_max:
            d_max, idx = d, i

    if d_max > epsilon_m:
        left = rdp_simplify(points[: idx + 1], epsilon_m)
        right = rdp_simplify(points[idx:], epsilon_m)
        return left[:-1] + right
    return [start, end]


def simplify_track(track: pd.DataFrame, *, epsilon_m: float) -> pd.DataFrame:
    """RDP-simplify one vessel's track (already sorted by t). Returns the
    kept rows (a subset of the input rows, original lat/lon/mmsi/t) in the
    projection used for simplification; the local equirectangular origin is
    the track's own centroid so a single-vessel call is self-contained."""
    if len(track) < 3:
        return track.copy()

    lat0, lon0 = track["lat"].mean(), track["lon"].mean()
    x, y = _equirectangular_xy(
        track["lat"].to_numpy(), track["lon"].to_numpy(), lat0=lat0, lon0=lon0
    )
    # zip()'s strict= keyword needs Python 3.10+; EMR Serverless's emr-7.2.0
    # Spark image ships Python 3.9, which raises "TypeError: zip() takes no
    # keyword arguments" (a real, first-live-EMR-run finding, W2 sprint
    # window, 2026-07-04). x and y come from the same _equirectangular_xy
    # call and are always equal length, so plain zip() is equivalent here.
    kept_xy = set(rdp_simplify(list(zip(x, y)), epsilon_m))  # noqa: B905
    mask = [(xi, yi) in kept_xy for xi, yi in zip(x, y)]  # noqa: B905
    return track.loc[mask].copy()


def simplify_all_tracks(df: pd.DataFrame, *, epsilon_m: float) -> pd.DataFrame:
    """simplify_track per MMSI, concatenated back into one frame. This is the
    function a Spark job would call via groupBy('mmsi').applyInPandas(...)."""
    parts = [simplify_track(g, epsilon_m=epsilon_m) for _, g in df.groupby("mmsi", sort=False)]
    if not parts:
        return df.iloc[0:0].copy()
    return pd.concat(parts, ignore_index=True)


def cluster_waypoints(simplified: pd.DataFrame, *, min_cluster_size: int = 2) -> pd.DataFrame:
    """HDBSCAN-cluster the RDP-kept turn points (across all vessels) into
    waypoint nodes. Returns corridor_graph_nodes: node_id, lat, lon (cluster
    centroid), vessel_count. Noise points (label -1, scikit-learn's HDBSCAN
    convention) are dropped: a turn seen nowhere else is not a shared
    waypoint. A projection centered on the whole simplified set's centroid is
    used so cluster distances are physically meaningful (meters, not
    degrees)."""
    from sklearn.cluster import HDBSCAN

    if simplified.empty:
        return pd.DataFrame(columns=["node_id", "lat", "lon", "vessel_count"])

    lat0, lon0 = simplified["lat"].mean(), simplified["lon"].mean()
    x, y = _equirectangular_xy(
        simplified["lat"].to_numpy(), simplified["lon"].to_numpy(), lat0=lat0, lon0=lon0
    )
    coords = np.column_stack([x, y])

    labels = HDBSCAN(min_cluster_size=min_cluster_size, copy=True).fit_predict(coords)
    working = simplified.assign(_cluster=labels)
    clustered = working[working["_cluster"] != -1]

    if clustered.empty:
        return pd.DataFrame(columns=["node_id", "lat", "lon", "vessel_count"])

    nodes = (
        clustered.groupby("_cluster")
        .agg(lat=("lat", "mean"), lon=("lon", "mean"), vessel_count=("mmsi", "nunique"))
        .reset_index(drop=True)
    )
    nodes.insert(0, "node_id", [f"node-{i}" for i in range(len(nodes))])

    # attach node_id back onto the working frame so derive_edges can walk each
    # vessel's ordered sequence of node assignments
    # No strict= (Python 3.9 on EMR, see simplify_track's comment); nodes is
    # built directly from the sorted unique cluster labels, so the two are
    # always equal length.
    label_to_node_id = {
        cluster_label: node_id
        for cluster_label, node_id in zip(  # noqa: B905
            sorted(clustered["_cluster"].unique()), nodes["node_id"]
        )
    }
    simplified.attrs["_cluster_labels"] = working["_cluster"].to_numpy()
    simplified.attrs["_label_to_node_id"] = label_to_node_id
    return nodes


def derive_edges(simplified: pd.DataFrame, nodes: pd.DataFrame) -> pd.DataFrame:
    """Given the simplified turn points (with cluster labels attached by
    cluster_waypoints via simplified.attrs) and the resulting node table,
    connect each vessel's consecutive DISTINCT node assignments into edges,
    aggregated to (from_node, to_node, frequency). Consecutive repeats of the
    same node (a vessel lingering near one waypoint) collapse to no edge;
    noise-labeled points (not assigned to any node) are skipped."""
    if nodes.empty or "_label_to_node_id" not in simplified.attrs:
        return pd.DataFrame(columns=["from_node", "to_node", "frequency"])

    label_to_node_id: dict = simplified.attrs["_label_to_node_id"]
    cluster_labels = simplified.attrs["_cluster_labels"]

    working = simplified.assign(_cluster=cluster_labels)
    edge_counts: dict[tuple[str, str], int] = {}
    for _, track in working.groupby("mmsi", sort=False):
        node_seq = [
            label_to_node_id[label] for label in track["_cluster"] if label in label_to_node_id
        ]
        # collapse consecutive repeats (lingering near one waypoint is not a transit)
        collapsed = [n for i, n in enumerate(node_seq) if i == 0 or n != node_seq[i - 1]]
        for a, b in zip(collapsed, collapsed[1:]):  # noqa: B905 (Python 3.9 on EMR)
            edge_counts[(a, b)] = edge_counts.get((a, b), 0) + 1

    if not edge_counts:
        return pd.DataFrame(columns=["from_node", "to_node", "frequency"])

    return (
        pd.DataFrame(
            [{"from_node": a, "to_node": b, "frequency": n} for (a, b), n in edge_counts.items()]
        )
        .sort_values(["from_node", "to_node"])
        .reset_index(drop=True)
    )


def derive_corridor_graph(
    df: pd.DataFrame, *, epsilon_m: float = 200.0, min_cluster_size: int = 2
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run the full lane pipeline on canonicalized positions.

    The steps are per-vessel RDP simplification, cross-vessel HDBSCAN waypoint
    clustering, and edge derivation. The function returns
    (corridor_graph_nodes, corridor_graph_edges)."""
    simplified = simplify_all_tracks(df, epsilon_m=epsilon_m)
    nodes = cluster_waypoints(simplified, min_cluster_size=min_cluster_size)
    edges = derive_edges(simplified, nodes)
    return nodes, edges
