"""Gate 3.3 smoke: point-in-time training-set export against the committed
fixture, no Feast, no AWS. Confirms the exported row count and data
fingerprint match the pinned golden values.

Usage: .venv/bin/python scripts/lake_training_export_smoke.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lake.export_training_set import export_training_set

FIXTURE = (
    Path(__file__).resolve().parent.parent / "lake" / "fixtures" / "marinecadastre_sample.jsonl"
)


def main() -> int:
    rows = [json.loads(line) for line in FIXTURE.read_text().splitlines() if line.strip()]
    feature_events = pd.DataFrame(rows)
    feature_events["t"] = pd.to_datetime(feature_events["t"], utc=True)

    # three synthetic point-in-time requests for the fixture's one vessel,
    # spanning before/within/after its recorded history
    mmsi = int(feature_events["mmsi"].iloc[0])
    entity_requests = pd.DataFrame(
        {
            "mmsi": [mmsi, mmsi, mmsi],
            "event_timestamp": pd.to_datetime(
                [
                    "2024-05-31T23:59:00Z",  # before any history: nulls expected
                    "2024-06-01T00:04:30Z",  # mid-history
                    "2024-06-01T00:30:00Z",  # after the last recorded fix
                ],
                utc=True,
            ),
        }
    )

    exported, fingerprint = export_training_set(entity_requests, feature_events)

    print(f"[PASS] export_training_set: {len(exported)} rows")
    print(f"        fingerprint: {fingerprint}")

    pinned = json.loads((FIXTURE.parent / "expectations.json").read_text())["training_export_smoke"]
    if len(exported) != pinned["row_count"] or fingerprint != pinned["data_fingerprint_sha256"]:
        print(
            "[FAIL] does not match lake/fixtures/expectations.json's "
            "training_export_smoke (row_count/data_fingerprint_sha256)"
        )
        return 1

    print("[PASS] matches the pinned checksum")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
