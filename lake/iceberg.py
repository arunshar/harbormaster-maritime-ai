"""Iceberg writer for the Phase 3 lake tables (ais_history, corridor_graph_nodes,
corridor_graph_edges). Extends the cdc/sinks/iceberg_audit.py catalog pattern
(same catalog_props shape: local SQLite catalog + file warehouse, or Glue
catalog + the S3 lake bucket on AWS) generalized to more than one fixed
table/schema, rather than duplicating a bespoke writer per table.

Three properties this module owns for a backfill that may be retried:

1. Partitioning. Each table is created with a PartitionSpec so writes land in
   pruneable partitions. The production intent for ais_history is a day(t)
   time partition plus a bucket(mmsi) fan-out; nodes/edges partition by their
   key prefix. See _partition_fields on each _LakeTableSpec.

2. Idempotency. A re-run of the same backfill must not double-write. The writer
   upserts on each table's natural key (ais_history: mmsi+t; nodes: node_id;
   edges: from_node+to_node) so re-appending identical rows leaves the row
   count stable (skip-if-present for new keys, no-op for unchanged rows).
   pyiceberg 0.11.1 exposes Table.upsert(join_cols=...), so we use MERGE-style
   upsert directly rather than delete-by-key + append.

3. Maintenance. run_lake_maintenance() compacts small data files (read-all +
   overwrite coalesces the per-append fragments into one file per partition)
   and expires old snapshots, bounding read and write amplification.

Read/write amplification note:
   Each append writes one (or a few) small Parquet data files and one new
   snapshot/manifest; a retried or incremental backfill therefore accretes many
   small files, which inflates read amplification (a scan opens every file) and
   metadata amplification (every snapshot is retained for time-travel). upsert
   adds write amplification of its own: matched rows are rewritten, not patched
   in place. Compaction trades a one-time bulk rewrite (high transient write
   amplification) for durably lower read amplification, and expire_snapshots
   drops the manifest/snapshot backlog. pyiceberg 0.11.1 has no native
   rewrite_data_files, so compaction here is an explicit read-all + overwrite.

pyiceberg 0.11.1 environment note:
   The day/hour/bucket partition transforms compute their partition values
   through the Rust `pyiceberg_core` extension on the write path; when that
   extra is not installed, a partitioned append/upsert raises NotInstalledError.
   To keep the local backfill smoke and the unit/e2e drills runnable without the
   native extension while still exercising a real, non-empty PartitionSpec, the
   spec falls back to an identity partition on the first key column when
   `pyiceberg_core` is absent. On AWS (extra installed) the intended
   day+bucket / key-prefix spec is used. This divergence is intentional, not a
   silent downgrade; _resolve_partition_fields() is the single decision point.
"""

from __future__ import annotations

import datetime as _dt
import importlib.util
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

AIS_HISTORY_TABLE = "ais_history"
CORRIDOR_NODES_TABLE = "corridor_graph_nodes"
CORRIDOR_EDGES_TABLE = "corridor_graph_edges"


def ais_history_schema():
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("mmsi", pa.int64(), nullable=False),
            pa.field("t", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("lat", pa.float64(), nullable=False),
            pa.field("lon", pa.float64(), nullable=False),
            pa.field("sog", pa.float64()),
            pa.field("cog", pa.float64()),
        ]
    )


def corridor_nodes_schema():
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("node_id", pa.string(), nullable=False),
            pa.field("lat", pa.float64(), nullable=False),
            pa.field("lon", pa.float64(), nullable=False),
            pa.field("vessel_count", pa.int64(), nullable=False),
        ]
    )


def corridor_edges_schema():
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("from_node", pa.string(), nullable=False),
            pa.field("to_node", pa.string(), nullable=False),
            pa.field("frequency", pa.int64(), nullable=False),
        ]
    )


@dataclass(frozen=True)
class _PartitionField:
    """One partition column: (source_column, transform, partition_name).

    transform is a pyiceberg transform string ("day", "hour", "bucket[16]",
    "identity"). `needs_core` marks transforms whose write path requires the
    pyiceberg_core Rust extension in pyiceberg 0.11.1.
    """

    source: str
    transform: str
    name: str
    needs_core: bool


@dataclass(frozen=True)
class _LakeTableSpec:
    schema_fn: Callable[[], Any]
    # dedup/natural key: re-appending rows with the same key values is a no-op
    dedup_keys: tuple[str, ...]
    # intended production partitioning
    partition_fields: tuple[_PartitionField, ...] = field(default_factory=tuple)


# Registry: schema + natural key + intended partition spec per table.
# ais_history: day(t) time partition + bucket(mmsi) fan-out is the standard
#   layout for point-in-time AIS pings (prune by day, spread hot vessels).
# nodes/edges: partition by the key prefix; identity keeps it core-free.
_LAKE_TABLES: dict[str, _LakeTableSpec] = {
    AIS_HISTORY_TABLE: _LakeTableSpec(
        schema_fn=ais_history_schema,
        dedup_keys=("mmsi", "t"),
        partition_fields=(
            _PartitionField("t", "day", "t_day", needs_core=True),
            _PartitionField("mmsi", "bucket[16]", "mmsi_bucket", needs_core=True),
        ),
    ),
    CORRIDOR_NODES_TABLE: _LakeTableSpec(
        schema_fn=corridor_nodes_schema,
        dedup_keys=("node_id",),
        partition_fields=(_PartitionField("node_id", "identity", "node_id", needs_core=False),),
    ),
    CORRIDOR_EDGES_TABLE: _LakeTableSpec(
        schema_fn=corridor_edges_schema,
        dedup_keys=("from_node", "to_node"),
        partition_fields=(_PartitionField("from_node", "identity", "from_node", needs_core=False),),
    ),
}

# Back-compat alias: callers importing TABLE_SCHEMAS still get the schema map.
TABLE_SCHEMAS: dict[str, Callable[[], Any]] = {
    name: spec.schema_fn for name, spec in _LAKE_TABLES.items()
}


def _pyiceberg_core_available() -> bool:
    return importlib.util.find_spec("pyiceberg_core") is not None


def _resolve_partition_fields(spec: _LakeTableSpec) -> tuple[_PartitionField, ...]:
    """The partition fields to actually apply given the runtime.

    If any intended field needs pyiceberg_core and the extension is missing,
    fall back to a single identity partition on the first dedup key so the write
    path stays functional and the table still carries a real, non-empty spec.
    This is the documented divergence from the module docstring.
    """
    if not spec.partition_fields:
        return ()
    if all(not pf.needs_core for pf in spec.partition_fields) or _pyiceberg_core_available():
        return spec.partition_fields
    key = spec.dedup_keys[0]
    return (_PartitionField(key, "identity", key, needs_core=False),)


def _apply_partition_spec(table: Any, fields: tuple[_PartitionField, ...]) -> None:
    if not fields:
        return
    with table.update_spec() as update:
        for pf in fields:
            if pf.transform == "identity":
                update.add_identity(pf.source)
            else:
                update.add_field(pf.source, pf.transform, pf.name)


def _load_or_create_table(catalog: Any, identifier: str, spec: _LakeTableSpec) -> Any:
    """Load the table if present, else create it and apply its PartitionSpec.

    Creation and spec application happen once (first backfill); later runs load
    the already-partitioned table, so re-running is spec-stable.
    """
    schema = spec.schema_fn()
    try:
        return catalog.load_table(identifier)
    except Exception:
        table = catalog.create_table(identifier, schema=schema)
        _apply_partition_spec(table, _resolve_partition_fields(spec))
        # reload so the returned handle carries the committed spec
        return catalog.load_table(identifier)


def _open_catalog(catalog_props: dict[str, str], namespace: str, table_name: str) -> Any:
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(f"hm_lake_{table_name}", **catalog_props)
    try:
        catalog.create_namespace_if_not_exists(namespace)
    except AttributeError:  # older pyiceberg
        try:
            catalog.create_namespace(namespace)
        except Exception:  # nosec B110  # idempotent namespace creation, already-exists is the expected benign case
            pass
    return catalog


def build_lake_writer(
    *, catalog_props: dict[str, str], namespace: str = "hm", table_name: str
) -> Callable[[list[dict[str, Any]]], None]:
    """The real pyiceberg appender for a lake table. catalog_props examples:

    local (backfill smoke / drills):
        {"type": "sql",
         "uri": "sqlite:///.lake-warehouse/catalog.db",
         "warehouse": "file://.lake-warehouse"}
    AWS showcase (Athena-queryable, Feast's offline store reads through here):
        {"type": "glue", "warehouse": "s3://<lake-bucket>/iceberg"}

    The returned writer is run-level idempotent: it upserts on the table's
    natural key, so re-running the same backfill leaves the row count stable
    instead of doubling it.
    """
    import pyarrow as pa

    if table_name not in _LAKE_TABLES:
        raise ValueError(f"no schema registered for lake table: {table_name}")
    spec = _LAKE_TABLES[table_name]
    schema = spec.schema_fn()

    catalog = _open_catalog(catalog_props, namespace, table_name)
    identifier = f"{namespace}.{table_name}"
    table = _load_or_create_table(catalog, identifier, spec)

    def writer(rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        arrow = pa.Table.from_pylist(rows, schema=schema)
        # MERGE-style idempotency: existing keys update in place, new keys
        # insert; a re-run of the same rows is a no-op on row count.
        table.upsert(arrow, join_cols=list(spec.dedup_keys))

    return writer


def run_lake_maintenance(
    *,
    catalog_props: dict[str, str],
    namespace: str = "hm",
    table_names: list[str] | None = None,
    expire_older_than: _dt.datetime | None = None,
    compact: bool = True,
) -> dict[str, dict[str, int]]:
    """Compact small data files and expire old snapshots for the lake tables.

    compaction: read the whole table and overwrite it, coalescing the many
        small per-append/upsert data files into one file per partition.
        pyiceberg 0.11.1 has no native rewrite_data_files, so this explicit
        read-all + overwrite is the coalesce path.
    expire_snapshots: drop snapshots older than `expire_older_than` (default:
        now, i.e. retain only the current snapshot), reclaiming the
        manifest/snapshot backlog.

    Returns per-table {data_files_before, data_files_after, snapshots_before,
    snapshots_after} so a caller (or a test) can assert files/snapshots dropped.
    """
    from pyiceberg.exceptions import NoSuchTableError

    names = table_names or list(_LAKE_TABLES)
    report: dict[str, dict[str, int]] = {}

    for table_name in names:
        if table_name not in _LAKE_TABLES:
            raise ValueError(f"no schema registered for lake table: {table_name}")
        catalog = _open_catalog(catalog_props, namespace, table_name)
        identifier = f"{namespace}.{table_name}"
        try:
            table = catalog.load_table(identifier)
        except NoSuchTableError:
            # nothing written yet; nothing to maintain
            continue

        files_before = len(list(table.scan().plan_files()))
        snaps_before = len(list(table.snapshots()))

        if compact and files_before > 1:
            data = table.scan().to_arrow()
            if data.num_rows > 0:
                # AlwaysTrue overwrite = rewrite every row into coalesced files
                table.overwrite(data)
                table = catalog.load_table(identifier)

        # Cutoff: an explicit caller value wins; otherwise expire everything
        # strictly older than the current snapshot. expire_snapshots always
        # protects the current (branch-head) snapshot, so this retains exactly
        # the live snapshot and sweeps the rest. Using the current snapshot's
        # own timestamp (not wall-clock now()) avoids a race where a just-
        # committed compaction snapshot shares the same millisecond as now().
        if expire_older_than is not None:
            when = expire_older_than
        else:
            current = table.current_snapshot()
            when = (
                _dt.datetime.fromtimestamp(current.timestamp_ms / 1000, tz=_dt.UTC)
                if current is not None
                else _dt.datetime.now(_dt.UTC)
            )

        table.maintenance.expire_snapshots().older_than(when).commit()
        table = catalog.load_table(identifier)

        report[table_name] = {
            "data_files_before": files_before,
            "data_files_after": len(list(table.scan().plan_files())),
            "snapshots_before": snaps_before,
            "snapshots_after": len(list(table.snapshots())),
        }

    return report
