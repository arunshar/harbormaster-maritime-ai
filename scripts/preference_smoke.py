"""Gate 4.4 smoke: build preference triples from the committed HITL fixture,
print counts per source.

Usage: .venv/bin/python scripts/preference_smoke.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from mlops.preference_builder import build_from_hitl

FIXTURES = Path(__file__).resolve().parent.parent / "mlops" / "fixtures"
FIXTURE = FIXTURES / "preference_hitl_rows.json"
EXPECTATIONS = FIXTURES / "preference_expectations.json"


def _fixed_now() -> str:
    return "2026-07-04T00:00:00+00:00"


def main() -> int:
    fixture = json.loads(FIXTURE.read_text())
    expected = json.loads(EXPECTATIONS.read_text())

    triples = build_from_hitl(
        fixture["rows"],
        contexts={},
        hitl_threshold=fixture["hitl_threshold"],
        now=_fixed_now,
    )

    by_source: dict[str, int] = {}
    for t in triples:
        by_source[t.preference_source] = by_source.get(t.preference_source, 0) + 1
    print(f"triples: {len(triples)}  by_source: {by_source}")

    ok = len(triples) == expected["triple_count"]
    if triples:
        first_dict = triples[0].to_dict()
        first_json = json.dumps(first_dict, sort_keys=True)
        print(f"first triple: {first_json}")
        ok = ok and first_json == expected["first_triple_json"]

    print("[PASS]" if ok else "[FAIL]", f"expected {expected['triple_count']} triples")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
