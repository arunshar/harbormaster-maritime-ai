"""Drill M-tenant-leak: a cross-tenant read is rejected by Postgres RLS itself,
not an application-layer check (Phase 5, gate 5.9; war story P36 in
docs/WAR_STORIES.md).

Reuses the gate 5.4 smoke's live RLS machinery verbatim
(scripts/phase5_tenant_smoke.rls_fail_closed_check) against a REAL Postgres, so
the drill and the smoke cannot diverge: it seeds tenant A's rows, switches the
session's app.tenant_id to tenant B, and proves tenant B reads zero of A's rows,
that a session with NO tenant set reads zero rows from every table (fail-closed,
not fail-open), and that the no-tenant write is rejected by the policy's WITH
CHECK. All as a NOSUPERUSER owner role, because superusers bypass RLS by
Postgres design.

Uses HM_TEST_PG_DSN when set; otherwise spins a throwaway postgres:16 container
(zero AWS, ~cents of local CPU). This is acceptance criterion (e) and the
grounding for incident P6. Transcript to docs/drills/M_tenant_leak.md.
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import phase5_tenant_smoke as smoke  # noqa: E402

TRANSCRIPT = REPO_ROOT / "docs" / "drills" / "M_tenant_leak.md"


def main() -> int:
    dsn = os.environ.get("HM_TEST_PG_DSN", "")
    started_docker = False
    lines: list[str] = [
        "# Drill M-tenant-leak transcript: RLS rejects a cross-tenant read "
        f"({datetime.now(UTC).isoformat()})",
        "",
    ]
    try:
        if not dsn:
            lines.append("No HM_TEST_PG_DSN; started a throwaway postgres:16 container (zero AWS).")
            dsn = smoke._docker_pg_up()
            started_docker = True
        asyncio.run(smoke._wait_ready(dsn))
        log = asyncio.run(smoke.rls_fail_closed_check(dsn))

        cross_tenant_blocked = any("cross-tenant blocked by RLS" in ln for ln in log)
        fail_closed = any("FAIL-CLOSED CONFIRMED" in ln for ln in log)
        write_rejected = any("WRITE rejected" in ln for ln in log)
        all_ok = cross_tenant_blocked and fail_closed and write_rejected

        lines.append("## RLS fail-closed checks (real Postgres, NOSUPERUSER owner)")
        for ln in log:
            lines.append(f"- {ln}")
        lines.append("")
        lines.append(f"cross_tenant_read_blocked: {cross_tenant_blocked}")
        lines.append(f"fail_closed_no_tenant_reads_zero: {fail_closed}")
        lines.append(f"no_tenant_write_rejected: {write_rejected}")
        lines.append("")
        lines.append(
            "VERDICT: "
            + (
                "PASS (cross-tenant isolation is enforced by the database, not by convention)"
                if all_ok
                else "FAIL"
            )
        )
        TRANSCRIPT.write_text("\n".join(lines) + "\n")
        print("\n".join(lines))
        return 0 if all_ok else 1
    except Exception as exc:
        lines.append(f"VERDICT: ERROR ({exc})")
        TRANSCRIPT.write_text("\n".join(lines) + "\n")
        print("\n".join(lines))
        raise
    finally:
        if started_docker:
            smoke._docker_pg_down()
            print("throwaway postgres container removed")


if __name__ == "__main__":
    raise SystemExit(main())
