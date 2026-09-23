"""Feast offline-store declarations for the AWS showcase (Phase 3, gate 3.3).

Feast 0.64 has no native or contrib Iceberg offline store (checked directly:
`feast.infra.offline_stores` and its `contrib/` package list bigquery, dask,
duckdb, redshift, snowflake, postgres, spark, trino, and others, no Iceberg),
but it does ship a contrib AthenaOfflineStore, and Athena is Iceberg-native:
the same Glue-catalog `ais_history` table `lake/iceberg.py` writes is
directly queryable through Athena. This module is declarative Feast config
only, wired to that table; it needs live Athena to actually run and is not
exercised locally (feast is declared in the `lake-emr` extra, not `lake`, and
is not installed in this dev venv - see docs/phases/PHASE_3.md). The real,
locally-testable point-in-time-join logic lives in
lake/export_training_set.py, independent of Feast's own join implementation.

Feast imports are deferred into functions so this module can be read, linted,
and imported by nothing without feast installed.
"""

from __future__ import annotations

GLUE_DATABASE = "hm"
ATHENA_TABLE = "ais_history"
ENTITY_NAME = "mmsi"


def build_ais_history_source(*, athena_data_source: str, workgroup: str):
    """The AthenaSource wired to the Iceberg-backed ais_history table."""
    from feast.infra.offline_stores.contrib.athena_offline_store.athena_source import (
        AthenaSource,
    )

    return AthenaSource(
        name="ais_history_source",
        table=ATHENA_TABLE,
        database=GLUE_DATABASE,
        timestamp_field="t",
        data_source=athena_data_source,
        description="Iceberg-backed ais_history, queried through Athena (workgroup "
        + workgroup
        + ")",
    )


def build_vessel_entity():
    from feast import Entity
    from feast.value_type import ValueType

    return Entity(
        name=ENTITY_NAME,
        join_keys=[ENTITY_NAME],
        value_type=ValueType.INT64,
        description="A vessel, keyed by MMSI",
    )


def build_ais_history_feature_view(*, athena_data_source: str, workgroup: str, ttl_days: int = 1):
    import datetime

    from feast import FeatureView, Field
    from feast.types import Float64

    return FeatureView(
        name="ais_history_features",
        entities=[build_vessel_entity()],
        ttl=datetime.timedelta(days=ttl_days),
        schema=[
            Field(name="lat", dtype=Float64),
            Field(name="lon", dtype=Float64),
            Field(name="sog", dtype=Float64),
            Field(name="cog", dtype=Float64),
        ],
        source=build_ais_history_source(athena_data_source=athena_data_source, workgroup=workgroup),
        online=False,
        offline=True,
        description="Point-in-time vessel position/kinematics features for the Pi-DPM export",
    )
