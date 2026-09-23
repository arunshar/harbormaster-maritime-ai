"""Gate 3.2 smoke: GE gate -> canonicalize -> corridor derivation -> Iceberg
write, end to end in plain Python (no Spark, no Java) against the committed
fixture, on the local SQLite catalog (mirrors cdc_smoke.py's local warehouse
convention).

The committed fixture is a single vessel's short northeast run: it exercises
the full pipeline's plumbing honestly, but a single vessel cannot produce a
"shared" corridor waypoint (HDBSCAN needs multiple vessels near the same
point to call it shared) - zero corridor nodes/edges is the CORRECT, expected
result here, not a failure; the multi-vessel corridor-discovery behavior is
covered by lake/tests/test_backfill_transforms.py's synthetic fixture.

Usage: .venv/bin/python scripts/lake_backfill_smoke.py
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lake.backfill.transforms import canonicalize_positions, derive_corridor_graph
from lake.iceberg import (
    AIS_HISTORY_TABLE,
    CORRIDOR_EDGES_TABLE,
    CORRIDOR_NODES_TABLE,
    build_lake_writer,
)
from lake.quality.marinecadastre_suite import validate_marinecadastre_batch

FIXTURE = (
    Path(__file__).resolve().parent.parent / "lake" / "fixtures" / "marinecadastre_sample.jsonl"
)
WAREHOUSE_DIR = Path(__file__).resolve().parent.parent / ".lake-warehouse"


def main() -> int:
    rows = [json.loads(line) for line in FIXTURE.read_text().splitlines() if line.strip()]
    df = pd.DataFrame(rows)

    gate = validate_marinecadastre_batch(df, min_rows=1)
    if not gate.passed:
        print(f"[FAIL] GE gate rejected {FIXTURE.name}:")
        for f in gate.failures:
            print(f"  - {f.name}: {f.detail}")
        return 1
    print(f"[PASS] GE gate: {gate.row_count} rows, 0 expectation failures")

    canonical = canonicalize_positions(df)
    print(f"[PASS] canonicalize_positions: {len(canonical)} ais_history rows")

    nodes, edges = derive_corridor_graph(canonical)
    print(f"[PASS] derive_corridor_graph: {len(nodes)} nodes, {len(edges)} edges")
    print("        (expected 0/0: a single vessel cannot produce a shared waypoint)")

    if WAREHOUSE_DIR.exists():
        shutil.rmtree(WAREHOUSE_DIR)
    WAREHOUSE_DIR.mkdir(parents=True)
    catalog_props = {
        "type": "sql",
        "uri": f"sqlite:///{WAREHOUSE_DIR}/catalog.db",
        "warehouse": f"file://{WAREHOUSE_DIR}",
    }

    write_ais_history = build_lake_writer(catalog_props=catalog_props, table_name=AIS_HISTORY_TABLE)
    write_ais_history(canonical.to_dict(orient="records"))
    write_nodes = build_lake_writer(catalog_props=catalog_props, table_name=CORRIDOR_NODES_TABLE)
    write_nodes(nodes.to_dict(orient="records"))
    write_edges = build_lake_writer(catalog_props=catalog_props, table_name=CORRIDOR_EDGES_TABLE)
    write_edges(edges.to_dict(orient="records"))
    print(f"[PASS] Iceberg writes complete on the local SQLite catalog at {WAREHOUSE_DIR}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
