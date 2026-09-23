"""Gate 3.1 smoke: run the MarineCadastre GE suite against the committed fixture.

Usage: .venv/bin/python scripts/lake_quality_smoke.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lake.quality.marinecadastre_suite import validate_marinecadastre_batch

FIXTURE = (
    Path(__file__).resolve().parent.parent / "lake" / "fixtures" / "marinecadastre_sample.jsonl"
)


def main() -> int:
    rows = [json.loads(line) for line in FIXTURE.read_text().splitlines() if line.strip()]
    df = pd.DataFrame(rows)
    result = validate_marinecadastre_batch(df, min_rows=1)

    if result.passed:
        print(f"[PASS] {FIXTURE.name}: {result.row_count} rows, 0 expectation failures")
        return 0

    print(f"[FAIL] {FIXTURE.name}: {result.row_count} rows")
    for f in result.failures:
        print(f"  - {f.name}: {f.detail}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
