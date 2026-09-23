"""Schema-shape tests plus real local-temp-warehouse tests for partitioning,
run-level idempotency, and maintenance (compaction + expire_snapshots).

The catalog-backed tests below use a SQLite catalog + file:// warehouse in a
tmp_path, so they are hermetic (no network, no AWS): the same local plane the
lake-backfill-smoke script uses. build_lake_writer's AWS/Glue path stays out of
the suite, mirroring cdc/sinks/iceberg_audit.py's build_iceberg_writer.
"""

from __future__ import annotations

import datetime as dt

from lake.iceberg import (
    AIS_HISTORY_TABLE,
    CORRIDOR_EDGES_TABLE,
    CORRIDOR_NODES_TABLE,
    TABLE_SCHEMAS,
    ais_history_schema,
    build_lake_writer,
    corridor_edges_schema,
    corridor_nodes_schema,
    run_lake_maintenance,
)


def test_all_registered_tables_have_a_schema():
    assert set(TABLE_SCHEMAS) == {AIS_HISTORY_TABLE, CORRIDOR_NODES_TABLE, CORRIDOR_EDGES_TABLE}


def test_ais_history_schema_field_names():
    assert ais_history_schema().names == ["mmsi", "t", "lat", "lon", "sog", "cog"]


def test_corridor_nodes_schema_field_names():
    assert corridor_nodes_schema().names == ["node_id", "lat", "lon", "vessel_count"]


def test_corridor_edges_schema_field_names():
    assert corridor_edges_schema().names == ["from_node", "to_node", "frequency"]


# --- catalog-backed tests: local SQLite catalog + file warehouse in tmp_path ---


def _local_catalog_props(tmp_path):
    warehouse = tmp_path / "lake-warehouse"
    warehouse.mkdir()
    return {
        "type": "sql",
        "uri": f"sqlite:///{warehouse}/catalog.db",
        "warehouse": f"file://{warehouse}",
    }


def _load_table(catalog_props, table_name, namespace="hm"):
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(f"hm_lake_{table_name}", **catalog_props)
    return catalog.load_table(f"{namespace}.{table_name}")


def _ais_rows(n):
    base = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)
    return [
        {
            "mmsi": 100 + (i % 3),
            "t": base + dt.timedelta(seconds=i),
            "lat": 44.0 + i * 0.001,
            "lon": -93.0 - i * 0.001,
            "sog": 5.0,
            "cog": 90.0,
        }
        for i in range(n)
    ]


def test_ais_table_carries_a_partition_spec(tmp_path):
    props = _local_catalog_props(tmp_path)
    writer = build_lake_writer(catalog_props=props, table_name=AIS_HISTORY_TABLE)
    writer(_ais_rows(4))

    table = _load_table(props, AIS_HISTORY_TABLE)
    spec = table.spec()
    assert not spec.is_unpartitioned()
    # at least one partition field bound to a real source column
    source_ids = {f.source_id for f in spec.fields}
    schema_ids = {f.field_id for f in table.schema().fields}
    assert source_ids <= schema_ids
    assert len(spec.fields) >= 1


def test_rerunning_the_same_append_is_idempotent(tmp_path):
    props = _local_catalog_props(tmp_path)
    writer = build_lake_writer(catalog_props=props, table_name=AIS_HISTORY_TABLE)
    rows = _ais_rows(6)

    writer(rows)
    first = _load_table(props, AIS_HISTORY_TABLE).scan().to_arrow().num_rows
    assert first == 6

    # same backfill re-run three more times -> row count stays stable
    writer(rows)
    writer(rows)
    writer(rows)
    after = _load_table(props, AIS_HISTORY_TABLE).scan().to_arrow().num_rows
    assert after == 6


def test_empty_write_is_a_noop(tmp_path):
    props = _local_catalog_props(tmp_path)
    writer = build_lake_writer(catalog_props=props, table_name=AIS_HISTORY_TABLE)
    writer(_ais_rows(2))
    writer([])
    assert _load_table(props, AIS_HISTORY_TABLE).scan().to_arrow().num_rows == 2


def test_new_keys_insert_existing_keys_do_not_duplicate(tmp_path):
    props = _local_catalog_props(tmp_path)
    writer = build_lake_writer(catalog_props=props, table_name=AIS_HISTORY_TABLE)
    rows = _ais_rows(4)
    writer(rows)
    # overlapping batch: first 4 rows repeat, 4 new rows added -> 8 total
    writer(_ais_rows(8))
    assert _load_table(props, AIS_HISTORY_TABLE).scan().to_arrow().num_rows == 8


def test_nodes_and_edges_writers_are_idempotent(tmp_path):
    props = _local_catalog_props(tmp_path)
    node_rows = [
        {"node_id": "n0", "lat": 44.0, "lon": -93.0, "vessel_count": 3},
        {"node_id": "n1", "lat": 45.0, "lon": -94.0, "vessel_count": 5},
    ]
    edge_rows = [{"from_node": "n0", "to_node": "n1", "frequency": 7}]

    write_nodes = build_lake_writer(catalog_props=props, table_name=CORRIDOR_NODES_TABLE)
    write_edges = build_lake_writer(catalog_props=props, table_name=CORRIDOR_EDGES_TABLE)
    for _ in range(3):
        write_nodes(node_rows)
        write_edges(edge_rows)

    assert _load_table(props, CORRIDOR_NODES_TABLE).scan().to_arrow().num_rows == 2
    assert _load_table(props, CORRIDOR_EDGES_TABLE).scan().to_arrow().num_rows == 1


def test_maintenance_compacts_files_and_expires_snapshots(tmp_path):
    props = _local_catalog_props(tmp_path)
    writer = build_lake_writer(catalog_props=props, table_name=AIS_HISTORY_TABLE)

    # many small distinct-key writes -> many small data files + many snapshots
    base = dt.datetime(2024, 2, 1, tzinfo=dt.UTC)
    for i in range(6):
        writer(
            [
                {
                    "mmsi": 200,
                    "t": base + dt.timedelta(seconds=i),
                    "lat": 44.0,
                    "lon": -93.0,
                    "sog": 5.0,
                    "cog": 90.0,
                }
            ]
        )

    before = _load_table(props, AIS_HISTORY_TABLE)
    files_before = len(list(before.scan().plan_files()))
    snaps_before = len(list(before.snapshots()))
    rows_before = before.scan().to_arrow().num_rows
    assert files_before > 1
    assert snaps_before > 1

    report = run_lake_maintenance(catalog_props=props, table_names=[AIS_HISTORY_TABLE])
    stats = report[AIS_HISTORY_TABLE]

    after = _load_table(props, AIS_HISTORY_TABLE)
    # rows preserved, files coalesced, snapshots reduced
    assert after.scan().to_arrow().num_rows == rows_before
    assert stats["data_files_after"] < stats["data_files_before"]
    assert stats["snapshots_after"] < stats["snapshots_before"]
    assert stats["snapshots_after"] == 1


def test_maintenance_on_missing_table_is_skipped(tmp_path):
    props = _local_catalog_props(tmp_path)
    # nothing written yet -> no table -> table absent from report, no error
    report = run_lake_maintenance(catalog_props=props, table_names=[AIS_HISTORY_TABLE])
    assert report == {}
