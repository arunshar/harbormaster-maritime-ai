# CorridorDeviationDetector: design and grounding

The CorridorDeviationDetector is the third deterministic agent in the Workflow 1 scoring path. This document is the design reference for it, and [HONESTY.md](HONESTY.md) carries the project framing.

## Honesty framing (read first)

Harbormaster is personal work that contains no employer code or data. The corridor detector is my own implementation, and the committed `serving/app/artifacts/corridors.json` is a small synthetic demo graph for the New York and New Jersey approaches. That graph was not built from NOAA charts or from any AIS extract.

## What the agent does

A vessel can pass the speed and gap checks and still behave unusually. It can leave the established sea lane, or it can turn sharply where no turn is expected. The CorridorDeviationDetector runs a corridor-graph association test against the static corridor graph. It emits two reasons into the same score-fusion and human-in-the-loop (HITL) review path as the gap and speed signals:

- `off_corridor`: the current fix's perpendicular distance to the nearest sea-lane edge exceeds `off_corridor_threshold_m` (2 km). Severity scales linearly to 1.0 at `corridor_saturation_m` (6 km).
- `unexpected_node`: a course change larger than `unexpected_node_heading_deg` (45 deg) occurs farther than `waypoint_radius_m` (5 km) from any expected waypoint node. Course changes near a waypoint are normal routing.

Both checks run inline on the CPU against a read-only artifact that loads once at startup, so they add no always-on cost.

## The artifact

`serving/app/artifacts/corridors.json` is a frozen sea-lane graph:

- `lanes`: This field holds sea-lane polylines as (lon, lat) node lists. The demo has one northeast approach lane.
- `waypoints`: This field holds the expected course-change nodes. The lane nodes double as waypoints.

The demo artifact is checked in so the slice runs offline. A larger artifact would come from running the RDP and HDBSCAN lane builder in `lake/backfill/transforms.py` over admitted historical AIS tracks. The publisher that would convert those lane tables into this JSON shape is assumed integration and does not exist in this repository, as [WORKFLOWS.md](WORKFLOWS.md) explains under Workflow 4. The synthetic fixture's off-corridor vessel (MMSI 200000003) runs about 10 km off this lane, and it is the documented `off_corridor` golden case in `streaming/fixtures/expectations.json`.

## Geometry

Distances use a local equirectangular projection centered on the lane centroid, which is Euclidean to first order over the demo region. The off-corridor test measures the perpendicular distance from the point to the nearest lane edge. The unexpected-node test compares the course over ground of the last two segments. For larger regions, a UTM zone (pyproj) or another metric projection should replace this local projection.

## Status

The corridor slice runs entirely locally and needs no AWS resources. The current loader reads lane geometry and does not validate a release version, so release selection and lineage remain proposed work.
