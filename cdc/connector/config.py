"""Debezium Postgres connector configuration (Phase 2, gate C3).

Generates the Kafka Connect JSON for io.debezium.connector.postgresql
.PostgresConnector against the Harbormaster registry, and validates it against
the schema's replication surface (cdc/schema/ddl.py is the single source of
truth for tables, publication, and slot).

Deliberate settings:
- publication.autocreate.mode=disabled: the publication is created by the DDL,
  so ownership of the replication surface stays with the schema, not the tool.
- tombstones.on.delete=true: deletes emit the op=d event AND the compaction
  tombstone; the consumer parses both (envelope.py) and applies only the former.
- heartbeat.interval.ms plus heartbeat.action.query: a private published
  heartbeat row advances the slot's confirmed_flush_lsn on idle AWS RDS
  instances, which is half of the P1 replication-slot-bloat story.
- snapshot.mode=initial: first start snapshots the tables (op=r events carrying
  LSNs), then streams; the applier's guard makes the transition duplicate-safe.
"""

from __future__ import annotations

from typing import Any

from cdc.schema.ddl import CDC_TABLES, HEARTBEAT_TABLE, PUBLICATION_NAME, SCHEMA, SLOT_NAME

CONNECTOR_NAME = "harbormaster-postgres"
TOPIC_PREFIX = "hm"
DATABASE_PASSWORD_REFERENCE = "${dir:/dev/shm/secrets:password}"  # nosec B105  # provider reference, not a credential


def topic_for(table: str, prefix: str = TOPIC_PREFIX) -> str:
    """The Kafka topic Debezium routes one table's changes to."""
    return f"{prefix}.{SCHEMA}.{table}"


def table_topics(prefix: str = TOPIC_PREFIX) -> tuple[str, ...]:
    return tuple(topic_for(t, prefix) for t in CDC_TABLES)


def heartbeat_topic(prefix: str = TOPIC_PREFIX) -> str:
    return f"__debezium-heartbeat.{prefix}"


# HEARTBEAT_TABLE is a schema constant from cdc.schema.ddl, not user input.
HEARTBEAT_ACTION_QUERY = (
    f"INSERT INTO public.{HEARTBEAT_TABLE} (id, updated_at) VALUES (1, now()) "  # nosec B608
    "ON CONFLICT (id) DO UPDATE SET updated_at = EXCLUDED.updated_at"
)


def build_connector_config(
    *,
    db_host: str,
    db_port: int = 5432,
    db_name: str = "harbormaster",
    db_user: str = "hm_admin",
    db_password: str = DATABASE_PASSWORD_REFERENCE,
    topic_prefix: str = TOPIC_PREFIX,
    snapshot_mode: str = "initial",
    heartbeat_interval_ms: int = 10_000,
) -> dict[str, Any]:
    """The Kafka Connect REST body registering the Harbormaster connector.

    db_password defaults to a Connect config-provider reference so real
    credentials never land in this repo or in the Connect REST history; pass a
    literal only in throwaway local stacks.
    """
    return {
        "name": CONNECTOR_NAME,
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",
            "database.hostname": db_host,
            "database.port": str(db_port),
            "database.dbname": db_name,
            "database.user": db_user,
            "database.password": db_password,
            "plugin.name": "pgoutput",
            "publication.name": PUBLICATION_NAME,
            "publication.autocreate.mode": "disabled",
            "slot.name": SLOT_NAME,
            "table.include.list": ",".join(f"{SCHEMA}.{t}" for t in CDC_TABLES),
            "topic.prefix": topic_prefix,
            "snapshot.mode": snapshot_mode,
            "tombstones.on.delete": "true",
            "heartbeat.interval.ms": str(heartbeat_interval_ms),
            "heartbeat.action.query": HEARTBEAT_ACTION_QUERY,
            "decimal.handling.mode": "string",
            "time.precision.mode": "adaptive_time_microseconds",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
        },
    }


REQUIRED_KEYS = (
    "connector.class",
    "database.hostname",
    "database.port",
    "database.dbname",
    "database.user",
    "database.password",
    "plugin.name",
    "publication.name",
    "publication.autocreate.mode",
    "slot.name",
    "table.include.list",
    "topic.prefix",
    "snapshot.mode",
    "tombstones.on.delete",
    "heartbeat.interval.ms",
    "heartbeat.action.query",
)


def validate_connector_config(body: dict[str, Any]) -> None:
    """Reject configs that drift from the schema's replication surface."""
    cfg = body.get("config")
    if not isinstance(cfg, dict):
        raise ValueError("connector body has no config object")
    missing = [k for k in REQUIRED_KEYS if k not in cfg]
    if missing:
        raise ValueError(f"connector config is missing required keys: {missing}")
    if cfg["plugin.name"] != "pgoutput":
        raise ValueError(f"plugin.name must be pgoutput, got {cfg['plugin.name']!r}")
    if cfg["publication.autocreate.mode"] != "disabled":
        raise ValueError(
            "publication.autocreate.mode must be disabled; the DDL owns the publication"
        )
    if cfg["publication.name"] != PUBLICATION_NAME:
        raise ValueError(f"publication.name must be {PUBLICATION_NAME}")
    if cfg["slot.name"] != SLOT_NAME:
        raise ValueError(f"slot.name must be {SLOT_NAME}")
    expected_tables = {f"{SCHEMA}.{t}" for t in CDC_TABLES}
    got_tables = {t.strip() for t in str(cfg["table.include.list"]).split(",") if t.strip()}
    if got_tables != expected_tables:
        raise ValueError(
            f"table.include.list {sorted(got_tables)} != schema surface {sorted(expected_tables)}"
        )
    if str(cfg["tombstones.on.delete"]).lower() != "true":
        raise ValueError("tombstones.on.delete must stay true; the consumer expects them")
    if int(cfg["heartbeat.interval.ms"]) <= 0:
        raise ValueError("heartbeat.interval.ms must be positive (slot-advance on idle)")
    if cfg["heartbeat.action.query"] != HEARTBEAT_ACTION_QUERY:
        raise ValueError("heartbeat.action.query must advance the private published heartbeat row")
