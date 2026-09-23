"""Re-record cdc/fixtures/debezium_envelopes.jsonl from the live local stack
(Phase 2, gate C6 closeout; see docs/phases/PHASE_2.md 2.3/2.6).

Drives the exact hand-authored scenario against the kind stack and captures
the real Debezium 2.7 output byte-for-byte from the Kafka topics:

  1. seed the two snapshot rows (vessels + watchlist for mmsi 200000001)
  2. register the connector (snapshot.mode=initial -> op=r reads)
  3. live DML: create watchlist 200000003, update its severity 0.9 -> 0.95,
     create vessels 200000003, create sanctions_flags 200000003:ofac,
     delete watchlist 200000001 (-> op=d + Kafka tombstone)
  4. capture everything (data topics + heartbeat + schema-change topic) with
     a from-earliest consumer, then compose the fixture in the canonical
     11-line order, with the create repeated verbatim as the redelivery line
     (a replayed offset hands the consumer the identical bytes)

Then re-pins every fixture-derived expectation in cdc/fixtures/expectations.json
(fixture SHA256, envelope census, apply census, final-state SHA256) and prints
the documented diffs vs the hand-authored fixture. Lines the live plane cannot
produce (a schema-change message; the schemas.enable=true wrapped variant) are
retained/covered as printed FINDINGS, not silently invented.

Preconditions: `make cdc-up` done and the connector NOT yet registered (the
snapshot capture needs a fresh slot). Run BEFORE `make cdc-smoke`.

The scenario's vessels and sanctions entry are fictional. Both MMSIs start with
the MID 200, which the ITU does not allocate to any country. The sanctions
reference SDN-EXAMPLE-0001 is a placeholder and is not a real list entry.

`--repin-only` skips the live capture and needs no stack, Kafka, or Postgres.
It re-derives the same expectations from the committed envelope file. A
deliberate fixture edit, such as the 2026-09-22 rewrite to fictional
identifiers, then gets its pins from this code, and no hash is edited by hand.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import urllib.request
from pathlib import Path

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from cdc.connector.config import (  # noqa: E402
    CONNECTOR_NAME,
    build_connector_config,
    heartbeat_topic,
)
from cdc.connector.registration import register_and_wait  # noqa: E402
from cdc.consumer.applier import Applier  # noqa: E402
from cdc.consumer.envelope import ChangeEvent, Skip, Tombstone, parse_envelope  # noqa: E402
from cdc.fixtures.loader import (  # noqa: E402
    ENVELOPES_PATH,
    EXPECTATIONS_PATH,
    envelopes_sha256,
    load_envelope_messages,
    load_expectations,
)
from cdc.schema import ddl  # noqa: E402
from cdc.sinks.base import MemoryAudit, MemorySink  # noqa: E402

PG_DSN = os.environ.get(
    "HM_RECORD_PG_DSN", "postgresql://hm_admin:hm_local_pw@127.0.0.1:30432/harbormaster"
)
CONNECT_URL = os.environ.get("HM_RECORD_CONNECT_URL", "http://127.0.0.1:30083")
PG_IN_CLUSTER_HOST = os.environ.get("HM_RECORD_PG_CLUSTER_HOST", "postgres.hm-cdc.svc")
BOOTSTRAP = os.environ.get("HM_KAFKA_BOOTSTRAP", "127.0.0.1:30092")
TIMEOUT_S = float(os.environ.get("HM_RECORD_TIMEOUT_S", "180"))
SCHEMA_TOPIC = "hm"  # topic.prefix; schema-change events land here when emitted

SNAPSHOT_MMSI = 200000001
LIVE_MMSI = 200000003


def connector_exists() -> bool:
    with urllib.request.urlopen(f"{CONNECT_URL}/connectors", timeout=10) as r:
        return CONNECTOR_NAME in json.loads(r.read())


def register_connector() -> None:
    body = build_connector_config(
        db_host=PG_IN_CLUSTER_HOST,
        db_port=5432,
    )
    result = register_and_wait(CONNECT_URL, body, timeout_s=TIMEOUT_S)
    tasks = ",".join(result.task_states)
    print(
        f"  [ok] connector registered (HTTP {result.http_status}; "
        f"connector={result.connector_state}; tasks={tasks})"
    )


async def seed_snapshot_rows() -> None:
    import asyncpg

    conn = await asyncpg.connect(dsn=PG_DSN)
    try:
        for stmt in ddl.statements():
            await conn.execute(stmt)
        await conn.execute(
            """
            INSERT INTO vessels (mmsi, name, flag_state, vessel_type, updated_at)
            VALUES ($1, 'HM DEMO VESSEL A', 'US', 'cargo', '2026-07-01T00:00:00Z')
            ON CONFLICT (tenant_id, mmsi) DO NOTHING
            """,
            SNAPSHOT_MMSI,
        )
        await conn.execute(
            """
            INSERT INTO watchlist (mmsi, reason, severity, added_by, created_at, updated_at)
            VALUES ($1, 'legacy flag', 0.5, 'seed',
                    '2026-06-01T00:00:00Z', '2026-06-01T00:00:00Z')
            ON CONFLICT (tenant_id, mmsi) DO NOTHING
            """,
            SNAPSHOT_MMSI,
        )
    finally:
        await conn.close()


async def run_live_dml() -> None:
    import asyncpg

    conn = await asyncpg.connect(dsn=PG_DSN)
    try:
        steps = [
            (
                """
                INSERT INTO watchlist (mmsi, reason, severity, added_by, created_at, updated_at)
                VALUES ($1, 'dark rendezvous', 0.9, 'arun',
                        '2026-07-03T12:00:00Z', '2026-07-03T12:00:00Z')
                """,
                LIVE_MMSI,
            ),
            (
                """
                UPDATE watchlist SET severity = 0.95, updated_at = '2026-07-03T12:05:00Z'
                WHERE tenant_id = '00000000-0000-0000-0000-000000000000'::uuid
                  AND mmsi = $1
                """,
                LIVE_MMSI,
            ),
            (
                """
                INSERT INTO vessels (mmsi, name, flag_state, vessel_type, updated_at)
                VALUES ($1, 'HM DEMO VESSEL B', 'PA', 'container', '2026-07-03T12:06:00Z')
                """,
                LIVE_MMSI,
            ),
            (
                """
                INSERT INTO sanctions_flags (id, mmsi, regime, reference, created_at, updated_at)
                VALUES ($1, $2, 'ofac', 'SDN-EXAMPLE-0001',
                        '2026-07-03T12:07:00Z', '2026-07-03T12:07:00Z')
                """,
                f"{LIVE_MMSI}:ofac",
                LIVE_MMSI,
            ),
            (
                """DELETE FROM watchlist
                   WHERE tenant_id = '00000000-0000-0000-0000-000000000000'::uuid
                     AND mmsi = $1""",
                SNAPSHOT_MMSI,
            ),
        ]
        for sql, *args in steps:
            await conn.execute(sql, *args)
            await asyncio.sleep(0.3)  # distinct transactions, stable per-topic order
    finally:
        await conn.close()


class Capture:
    """Raw (topic, key_str_or_None, value_str_or_None) messages plus their parse."""

    def __init__(self) -> None:
        self.messages: list[tuple[str, str | None, str | None]] = []

    def add(self, topic: str, key: bytes | None, value: bytes | None) -> None:
        self.messages.append(
            (topic, key.decode() if key else None, value.decode() if value is not None else None)
        )

    def find(self, want) -> tuple[str, str | None, str | None] | None:
        for topic, key, value in self.messages:
            try:
                parsed = parse_envelope(topic, key, value)
            except Exception:
                continue
            if want(topic, parsed):
                return (topic, key, value)
        return None


def build_recorder_consumer():
    from confluent_kafka import Consumer

    return Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"hm-fixture-recorder-{os.getpid()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "allow.auto.create.topics": False,
            # Debezium creates topics lazily; the default 5-minute metadata
            # refresh would make the regex subscription miss them for minutes.
            "topic.metadata.refresh.interval.ms": "5000",
        }
    )


def drain(consumer, cap: Capture, until, timeout_s: float, label: str) -> bool:
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        msg = consumer.poll(1.0)
        if msg is None:
            if until():
                return True
            continue
        if msg.error():
            continue
        cap.add(msg.topic(), msg.key(), msg.value())
        if until():
            return True
    print(f"  [warn] {label}: not satisfied after {timeout_s:.0f}s")
    return until()


def repin_expectations(write_to: Path | None = None) -> dict:
    """Re-derive every fixture-derived pin from the committed envelope file.

    The pins are the envelope SHA256, the envelope census, the final-state
    SHA256, and the apply census. Every other key in expectations.json is kept.
    When ``write_to`` is a path, the function writes the result there in the
    recorder's usual ``indent=2`` format. It always returns the result.
    """
    parsed = [parse_envelope(t, k, v) for t, k, v in load_envelope_messages()]
    store, audit = MemorySink(), MemoryAudit()
    result = Applier(store=store, audit=audit).apply_batch(parsed, commit=lambda: None)

    exp = load_expectations()
    exp["debezium_envelopes_sha256"] = envelopes_sha256()
    exp["envelope_census"] = {
        "change_events": sum(isinstance(p, ChangeEvent) for p in parsed),
        "tombstones": sum(isinstance(p, Tombstone) for p in parsed),
        "skips": sum(isinstance(p, Skip) for p in parsed),
    }
    exp["final_state_sha256"] = store.state_sha256()
    exp["apply_census"] = {
        "events": result.events,
        "applied": result.applied,
        "guard_rejected": result.guard_rejected,
        "tombstones": result.tombstones,
    }
    if write_to is not None:
        write_to.write_text(json.dumps(exp, indent=2) + "\n")
    return exp


def main(argv: list[str] | None = None) -> int:  # noqa: PLR0915
    args = sys.argv[1:] if argv is None else argv
    if args == ["--repin-only"]:
        exp = repin_expectations(EXPECTATIONS_PATH)
        print(f"re-pinned {EXPECTATIONS_PATH} from {ENVELOPES_PATH} (no live capture)")
        print(f"  sha256 {exp['debezium_envelopes_sha256'][:12]}...")
        print(f"  envelope census: {exp['envelope_census']}")
        print(f"  apply census:    {exp['apply_census']}")
        print(f"  final state:     {exp['final_state_sha256'][:12]}...")
        return 0
    if args:
        print("usage: cdc_record_fixture.py [--repin-only]")
        return 2

    if connector_exists() and os.environ.get("HM_RECORD_ALLOW_EXISTING") != "1":
        print(
            f"connector {CONNECTOR_NAME} already registered; the recorder needs a fresh\n"
            "stack so the snapshot (op=r) messages can be captured. Run right after\n"
            "`make cdc-up`, BEFORE `make cdc-smoke`. If the connector was registered but\n"
            "nothing consumed/changed since (snapshot messages still at the earliest\n"
            "offsets), HM_RECORD_ALLOW_EXISTING=1 skips this guard."
        )
        return 1

    old_lines = {}
    if ENVELOPES_PATH.exists():
        for line in ENVELOPES_PATH.read_text().splitlines():
            if line.strip():
                d = json.loads(line)
                if d["topic"] == SCHEMA_TOPIC:
                    old_lines["schema_change"] = d
                elif d["topic"].startswith("__debezium-heartbeat"):
                    old_lines["heartbeat"] = d
    old_sha = envelopes_sha256() if ENVELOPES_PATH.exists() else "(none)"

    print("seeding snapshot rows + DDL ...")
    asyncio.run(seed_snapshot_rows())
    print("registering the connector (initial snapshot) ...")
    register_connector()

    cap = Capture()
    consumer = build_recorder_consumer()
    consumer.subscribe(
        [
            "^hm\\.public\\.(vessels|watchlist|sanctions_flags)$",
            f"^{heartbeat_topic().replace('.', chr(92) + '.')}$",
            f"^{SCHEMA_TOPIC}$",
        ]
    )

    def _is(table, op, mmsi):
        def check(topic, parsed):
            return (
                isinstance(parsed, ChangeEvent)
                and parsed.table == table
                and parsed.op == op
                and parsed.pk.get("mmsi") == mmsi
            )

        return check

    snapshots_seen = lambda: (  # noqa: E731
        cap.find(_is("vessels", "r", SNAPSHOT_MMSI)) is not None
        and cap.find(_is("watchlist", "r", SNAPSHOT_MMSI)) is not None
    )
    print("waiting for the snapshot (op=r) messages ...")
    if not drain(consumer, cap, snapshots_seen, TIMEOUT_S, "snapshot capture"):
        return 1
    print("  [ok] snapshot captured; running the live DML ...")
    asyncio.run(run_live_dml())

    wanted = {
        "wl_create": _is("watchlist", "c", LIVE_MMSI),
        "wl_update": _is("watchlist", "u", LIVE_MMSI),
        "vessel_create": _is("vessels", "c", LIVE_MMSI),
        "sanctions_create": lambda t, p: (
            isinstance(p, ChangeEvent) and p.table == "sanctions_flags" and p.op == "c"
        ),
        "wl_delete": _is("watchlist", "d", SNAPSHOT_MMSI),
        "tombstone": lambda t, p: isinstance(p, Tombstone) and p.pk.get("mmsi") == SNAPSHOT_MMSI,
        "heartbeat": lambda t, p: isinstance(p, Skip) and p.reason == "heartbeat",
    }
    dml_seen = lambda: all(cap.find(w) is not None for w in wanted.values())  # noqa: E731
    print("waiting for the streamed changes + tombstone + a heartbeat ...")
    drain(consumer, cap, dml_seen, TIMEOUT_S, "stream capture")
    consumer.close()

    findings: list[str] = []

    def record_or_retain(name: str, want) -> dict | None:
        got = cap.find(want)
        if got is not None:
            topic, key, value = got
            return {
                "topic": topic,
                "key": json.loads(key) if key else None,
                "value": json.loads(value) if value is not None else None,
            }
        if name in old_lines:
            findings.append(
                f"{name}: not produced by the live plane; hand-authored line retained "
                "(the parser must still handle it: other planes/connectors emit these)"
            )
            return old_lines[name]
        print(f"  [fail] {name}: not captured and no hand-authored line to retain")
        return None

    def recorded(name: str, want) -> dict | None:
        got = cap.find(want)
        if got is None:
            print(f"  [fail] required message not captured: {name}")
            return None
        topic, key, value = got
        return {
            "topic": topic,
            "key": json.loads(key) if key else None,
            "value": json.loads(value) if value is not None else None,
        }

    lines = [
        recorded("vessels snapshot r", _is("vessels", "r", SNAPSHOT_MMSI)),
        recorded("watchlist snapshot r", _is("watchlist", "r", SNAPSHOT_MMSI)),
        record_or_retain("heartbeat", wanted["heartbeat"]),
        recorded("watchlist create", wanted["wl_create"]),
        recorded("watchlist update", wanted["wl_update"]),
        recorded("vessels create", wanted["vessel_create"]),
        recorded("sanctions create", wanted["sanctions_create"]),
        recorded("watchlist delete", wanted["wl_delete"]),
        recorded("tombstone", wanted["tombstone"]),
        record_or_retain(
            "schema_change",
            lambda t, p: t == SCHEMA_TOPIC and isinstance(p, Skip) and p.reason == "schema_change",
        ),
    ]
    if any(x is None for x in lines):
        return 1
    lines.append(dict(lines[3]))  # the redelivery: the create, byte-identical

    ENVELOPES_PATH.write_text("".join(json.dumps(d) + "\n" for d in lines))
    exp = repin_expectations(EXPECTATIONS_PATH)
    new_sha = exp["debezium_envelopes_sha256"]

    print(f"\nfixture re-recorded: {ENVELOPES_PATH}")
    print(f"  sha256 {old_sha[:12]}... -> {new_sha[:12]}...")
    print(f"  envelope census: {exp['envelope_census']}")
    print(f"  apply census:    {exp['apply_census']}")
    print(f"  final state:     {exp['final_state_sha256'][:12]}...")
    print("\ndocumented findings (diff vs the hand-authored fixture):")
    findings.append(
        "schemas.enable=false on the live converters, so no line is schema-wrapped; "
        "the wrapped-unwrap path keeps inline coverage in cdc/tests/test_envelope.py"
    )
    findings.append("LSNs/timestamps are now real values; position-pinned asserts relaxed")
    for f in findings:
        print(f"  - {f}")
    print("\nrun `make serve-test` to verify every re-pinned expectation, then commit the")
    print("fixture + expectations.json + this transcript in the same commit (gate 2.6).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
