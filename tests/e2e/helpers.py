"""Pure helpers for the Phase 1 e2e acceptance test (gate G8). Unit-tested; the
integration test in test_phase1.py uses them against a live demo apply."""

from __future__ import annotations


def anomaly_in_pending(pending: list[dict], mmsi: int) -> dict | None:
    """The pending HITL row for the given MMSI, or None."""
    for row in pending:
        try:
            if int(row.get("mmsi", -1)) == mmsi:
                return row
        except (TypeError, ValueError):
            continue
    return None


def within_slo(elapsed_s: float, slo_s: float) -> bool:
    return elapsed_s <= slo_s


def athena_count_query(database: str, table: str) -> str:
    return f'SELECT count(*) AS n FROM "{database}"."{table}"'


def reconciles(records_in: int, iceberg_count: int, gate_dropped: int = 0) -> bool:
    """Iceberg row count == fixture events minus the P_phys cheap-gate drops."""
    return iceberg_count == records_in - gate_dropped
