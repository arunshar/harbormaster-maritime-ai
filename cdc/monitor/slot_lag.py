"""Replication-slot lag monitoring, the pure core (Phase 2, gates C7/C8).

A logical replication slot is a contract with the database: while a consumer
is stalled, Postgres pins WAL from the slot's position and the disk fills
(war story P1). This module turns pg_replication_slots into typed lag numbers
and a threshold verdict; the slot-lag Lambda (infra/lambda/cdc_slot_lag)
publishes them as CloudWatch metrics, and drill P1 samples them live.

confirmed_flush_lsn is the correct lag anchor for a logical slot (everything
the subscriber has confirmed); restart_lsn is the fallback when confirmed is
null (slot created, consumer never connected), which is itself the worst-case
stall.

No I/O here: callers hand in rows (asyncpg / pg8000 / a test list) in the
SLOT_LAG_SQL column order.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

# Column order is the contract with rows_to_slot_lags().
SLOT_LAG_SQL = """
SELECT
    slot_name,
    active,
    COALESCE(
        pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn),
        pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
    ) AS lag_bytes
FROM pg_replication_slots
""".strip()

DEFAULT_LAG_ALARM_BYTES = 200 * 1024 * 1024  # 200 MB, mirrors the Terraform default


@dataclass(frozen=True)
class SlotLag:
    slot_name: str
    active: bool
    lag_bytes: int


def rows_to_slot_lags(rows: Iterable[Sequence[Any]]) -> list[SlotLag]:
    """Rows in SLOT_LAG_SQL order -> typed SlotLag values (null lag -> 0)."""
    out: list[SlotLag] = []
    for row in rows:
        slot_name, active, lag_bytes = row[0], row[1], row[2]
        out.append(
            SlotLag(
                slot_name=str(slot_name),
                active=bool(active),
                lag_bytes=0 if lag_bytes is None else int(lag_bytes),
            )
        )
    return out


def evaluate_lag_alert(
    slots: Iterable[SlotLag], threshold_bytes: int = DEFAULT_LAG_ALARM_BYTES
) -> list[SlotLag]:
    """The slots that breach the threshold (empty list == healthy). An INACTIVE
    slot with lag is the P1 signature: nothing is draining it and WAL is pinned."""
    if threshold_bytes <= 0:
        raise ValueError(f"threshold_bytes must be positive, got {threshold_bytes}")
    return [s for s in slots if s.lag_bytes >= threshold_bytes]


async def fetch_slot_lag(conn: Any) -> list[SlotLag]:
    """asyncpg convenience used by the drills: conn.fetch(SLOT_LAG_SQL)."""
    rows = await conn.fetch(SLOT_LAG_SQL)
    return rows_to_slot_lags([(r["slot_name"], r["active"], r["lag_bytes"]) for r in rows])
