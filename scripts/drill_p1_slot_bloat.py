"""Drill P1: replication-slot bloat (Phase 2, gate C8; war story P9 in
docs/WAR_STORIES.md).

Demonstrates the WAL-pinning mechanism live: a logical replication slot with
no consumer draining it (a stalled CDC consumer) pins WAL on the source, and
pg_replication_slots lag grows without bound while writes continue; the disk
on the source eventually fills. Then the recovery: draining the slot advances
confirmed_flush_lsn and the lag collapses.

Runs against any wal_level=logical Postgres:
  HM_DRILL_PG_DSN=postgresql://...   (e.g. the kind stack: port 30432)
or, with Docker available, spins a throwaway postgres:16 container.

Writes a transcript to docs/drills/P1_slot_bloat.md and exits 0 only if:
  - lag grew monotonically across the stalled rounds,
  - evaluate_lag_alert() fired at the drill threshold,
  - draining the slot reduced lag from its peak.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

from cdc.monitor.slot_lag import evaluate_lag_alert, fetch_slot_lag  # noqa: E402
from cdc.schema import ddl  # noqa: E402

# The drill NEVER touches the production slot (ddl.SLOT_NAME): pointed at a
# live CDC stack, creating/draining/dropping harbormaster_cdc would silently
# consume and then destroy Debezium's position. It uses its own slot and only
# warns when the production slot is present on the target.
DRILL_SLOT = "hm_drill_p1_slot_bloat"
DRILL_THRESHOLD_BYTES = 64 * 1024  # small so the drill fires quickly
ROUNDS = 5
ROWS_PER_ROUND = 300
DOCKER_NAME = "hm-drill-pg"
DOCKER_PORT = 55432
DOCKER_DSN = f"postgresql://hm_admin:hm_local_pw@127.0.0.1:{DOCKER_PORT}/harbormaster"
TRANSCRIPT = REPO_ROOT / "docs" / "drills" / "P1_slot_bloat.md"


def _docker_pg_up() -> str:
    subprocess.run(["docker", "rm", "-f", DOCKER_NAME], capture_output=True, check=False)
    subprocess.run(
        [
            "docker", "run", "-d", "--name", DOCKER_NAME,
            "-e", "POSTGRES_DB=harbormaster",
            "-e", "POSTGRES_USER=hm_admin",
            "-e", "POSTGRES_PASSWORD=hm_local_pw",  # throwaway, container-local
            "-p", f"127.0.0.1:{DOCKER_PORT}:5432",
            "postgres:16-alpine", "-c", "wal_level=logical",
        ],
        check=True,
        capture_output=True,
    )
    return DOCKER_DSN


def _docker_pg_down() -> None:
    subprocess.run(["docker", "rm", "-f", DOCKER_NAME], capture_output=True, check=False)


async def _wait_ready(dsn: str, timeout_s: float = 60.0) -> None:
    import asyncpg

    t0 = time.time()
    while True:
        try:
            conn = await asyncpg.connect(dsn=dsn, timeout=5)
            await conn.close()
            return
        except Exception:
            if time.time() - t0 > timeout_s:
                raise
            await asyncio.sleep(1.0)


async def run_drill(dsn: str, log: list[str]) -> None:
    import asyncpg

    conn = await asyncpg.connect(dsn=dsn)
    try:
        for stmt in ddl.statements():
            await conn.execute(stmt)
        await conn.execute(
            "SELECT set_config('app.tenant_id', $1, false)", ddl.DEFAULT_TENANT_ID
        )

        prod_slot = await conn.fetchval(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1", ddl.SLOT_NAME
        )
        if prod_slot:
            log.append(
                f"NOTE: production slot `{ddl.SLOT_NAME}` exists on this target (a live "
                "CDC stack); the drill leaves it strictly alone and uses its own slot"
            )

        # the "stalled consumer": a drill-only logical slot nothing drains
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1", DRILL_SLOT
        )
        if exists:
            raise RuntimeError(
                f"drill slot `{DRILL_SLOT}` already exists; a previous drill did not clean "
                "up. Drop it explicitly before re-running (never auto-adopt a slot)."
            )
        await conn.execute(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", DRILL_SLOT
        )
        log.append(f"drill slot `{DRILL_SLOT}` (pgoutput) created; NO consumer attached")

        baseline = await fetch_slot_lag(conn)
        log.append(f"baseline: {baseline}")

        samples: list[int] = []
        for rnd in range(1, ROUNDS + 1):
            for i in range(ROWS_PER_ROUND):
                mmsi = 200_810_000 + (rnd * ROWS_PER_ROUND) + i
                await conn.execute(
                    """
                    INSERT INTO watchlist (mmsi, reason, severity, added_by)
                    VALUES ($1, 'drill p1 wal generation', 0.5, 'drill_p1')
                    ON CONFLICT (tenant_id, mmsi) DO UPDATE SET updated_at = now()
                    """,
                    mmsi,
                )
            await conn.execute("SELECT pg_switch_wal()")
            slots = await fetch_slot_lag(conn)
            ours = next(s for s in slots if s.slot_name == DRILL_SLOT)
            samples.append(ours.lag_bytes)
            log.append(
                f"round {rnd}: +{ROWS_PER_ROUND} rows, pg_switch_wal -> "
                f"lag_bytes={ours.lag_bytes:,} active={ours.active}"
            )

        assert all(b > a for a, b in zip(samples, samples[1:], strict=False)), (
            f"lag did not grow monotonically while stalled: {samples}"
        )
        log.append(f"MONOTONIC GROWTH CONFIRMED across {len(samples)} samples: {samples}")

        breaching = evaluate_lag_alert(
            await fetch_slot_lag(conn), threshold_bytes=DRILL_THRESHOLD_BYTES
        )
        assert any(s.slot_name == DRILL_SLOT for s in breaching), "alert did not fire"
        log.append(
            f"ALERT FIRED: evaluate_lag_alert(threshold={DRILL_THRESHOLD_BYTES:,}) -> "
            f"{[s.slot_name for s in breaching]} (the CloudWatch alarm watches the same number)"
        )
        peak = samples[-1]

        # recovery: drain the slot (what a healthy consumer does continuously)
        await conn.fetch(
            "SELECT * FROM pg_logical_slot_get_binary_changes($1, NULL, NULL, "
            "'proto_version', '1', 'publication_names', $2)",
            DRILL_SLOT,
            ddl.PUBLICATION_NAME,
        )
        drained = await fetch_slot_lag(conn)
        ours = next(s for s in drained if s.slot_name == DRILL_SLOT)
        assert ours.lag_bytes < peak, f"drain did not reduce lag: {ours.lag_bytes} vs {peak}"
        log.append(
            f"RECOVERY: draining the slot dropped lag {peak:,} -> {ours.lag_bytes:,} bytes"
        )

        await conn.execute("SELECT pg_drop_replication_slot($1)", DRILL_SLOT)
        log.append("drill slot dropped (cleanup); production slot untouched")
    finally:
        await conn.close()


def main() -> int:
    log: list[str] = [
        f"# Drill P1 transcript: replication-slot bloat ({datetime.now(UTC).isoformat()})",
        "",
        "A logical slot with no consumer pins WAL; lag grows without bound while",
        "writes continue. Mechanism, alert, and recovery, sampled live below.",
        "",
    ]
    dsn = os.environ.get("HM_DRILL_PG_DSN", "")
    started_docker = False
    try:
        if not dsn:
            log.append("no HM_DRILL_PG_DSN; starting a throwaway postgres:16 container")
            dsn = _docker_pg_up()
            started_docker = True
        asyncio.run(_wait_ready(dsn))
        asyncio.run(run_drill(dsn, log))
        log.append("")
        log.append("VERDICT: PASS (monotonic growth, alert fired, drain recovered)")
        return 0
    except Exception as exc:
        log.append(f"VERDICT: FAIL ({exc})")
        raise
    finally:
        if started_docker:
            _docker_pg_down()
            log.append("throwaway postgres container removed")
        TRANSCRIPT.parent.mkdir(parents=True, exist_ok=True)
        TRANSCRIPT.write_text("\n".join(str(x) for x in log) + "\n")
        print("\n".join(str(x) for x in log))
        print(f"\ntranscript -> {TRANSCRIPT}")


if __name__ == "__main__":
    sys.exit(main())
