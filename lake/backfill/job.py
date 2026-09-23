"""EMR Serverless entrypoint for the Phase 3 MarineCadastre backfill (gate 3.2).

Thin orchestration only: every real transform lives in lake/backfill/transforms.py
and lake/quality/marinecadastre_suite.py as plain pandas/NumPy/scikit-learn
functions, exercised by the unit suite with zero Spark. This file is the glue
that runs those same functions per-partition/per-group inside a real Spark
job; it is EMR-only and deliberately NOT exercised by a local test (this dev
machine has no JVM, see the finding in docs/phases/PHASE_3.md). Submitted via
`aws emr-serverless start-job-run` at demo-apply time (operator-run), never as a
long-lived Terraform-managed resource.

Usage on EMR Serverless (spark-submit args, illustrative):
  --entry-point lake/backfill/job.py
  --entry-point-arguments <raw_extract_s3_uri> <catalog_type> <catalog_warehouse>
"""

from __future__ import annotations

import sys

from lake.backfill.transforms import canonicalize_positions, derive_corridor_graph
from lake.iceberg import (
    AIS_HISTORY_TABLE,
    CORRIDOR_EDGES_TABLE,
    CORRIDOR_NODES_TABLE,
    build_lake_writer,
)
from lake.quality.marinecadastre_suite import validate_marinecadastre_batch


class DataQualityGateFailure(RuntimeError):
    """Raised when the gate 3.1 suite rejects a partition; the job halts and
    nothing from that batch reaches Iceberg. Never caught silently."""


def _gate_and_canonicalize_partition(pdf):
    """Called per-partition via mapInPandas. Fails the whole job on a bad
    partition rather than writing partial/silent data (the explicit acceptance
    criterion: bad data blocks training)."""
    result = validate_marinecadastre_batch(pdf, min_rows=1)
    if not result.passed:
        details = "; ".join(f"{f.name}: {f.detail}" for f in result.failures)
        raise DataQualityGateFailure(f"MarineCadastre GE suite rejected a partition: {details}")
    return canonicalize_positions(pdf)


def run(spark, *, raw_extract_s3_uri: str, catalog_props: dict[str, str]) -> None:
    from pyspark.sql.types import (  # noqa: E501
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    raw_schema = StructType(
        [
            StructField("mmsi", LongType(), False),
            StructField("t", StringType(), False),
            StructField("lat", DoubleType(), False),
            StructField("lon", DoubleType(), False),
            StructField("sog", DoubleType(), True),
            StructField("cog", DoubleType(), True),
        ]
    )

    raw = spark.read.schema(raw_schema).parquet(raw_extract_s3_uri)

    # canonicalize_positions() (lake/backfill/transforms.py) parses "t" into a
    # real UTC timestamp as part of canonicalization; the mapInPandas output
    # schema must reflect that shape, not the raw input's. Reusing raw.schema
    # here (StringType for "t") made Spark try to convert the returned
    # datetime64 column back to Arrow as a string and fail with
    # "PySparkTypeError: ... datetime64[ns, UTC] ... Expected a string or
    # bytes dtype" -- a real, first-live-EMR-run finding (this glue code has
    # no local JVM to catch it against), W2 sprint window, 2026-07-04.
    canonical_schema = StructType(
        [
            StructField("mmsi", LongType(), False),
            StructField("t", TimestampType(), False),
            StructField("lat", DoubleType(), False),
            StructField("lon", DoubleType(), False),
            StructField("sog", DoubleType(), True),
            StructField("cog", DoubleType(), True),
        ]
    )

    # gate + canonicalize per partition; a failure here raises inside the Spark
    # executor and fails the job (no partial write)
    canonical = raw.mapInPandas(
        lambda it: (_gate_and_canonicalize_partition(pdf) for pdf in it),
        schema=canonical_schema,
    )

    ais_history_rows = canonical.toPandas().to_dict(orient="records")
    write_ais_history = build_lake_writer(catalog_props=catalog_props, table_name=AIS_HISTORY_TABLE)
    write_ais_history(ais_history_rows)

    # HDBSCAN clustering needs a cross-vessel, cross-partition view, so the
    # (already gated and canonicalized) positions are collected to the driver
    # for the corridor-graph derivation; at this project's scale (a personal,
    # demo-grade backfill, not petabyte AIS) this is a deliberate, documented
    # choice, not an oversight.
    full_df = canonical.toPandas()
    nodes, edges = derive_corridor_graph(full_df)

    write_nodes = build_lake_writer(catalog_props=catalog_props, table_name=CORRIDOR_NODES_TABLE)
    write_nodes(nodes.to_dict(orient="records"))

    write_edges = build_lake_writer(catalog_props=catalog_props, table_name=CORRIDOR_EDGES_TABLE)
    write_edges(edges.to_dict(orient="records"))


def main(argv: list[str]) -> int:
    import os

    from pyspark.sql import SparkSession

    raw_extract_s3_uri, catalog_type, catalog_warehouse = argv[1], argv[2], argv[3]
    aws_region = os.environ.get("AWS_REGION", "us-east-1")
    catalog_props = (
        # pyiceberg's GlueCatalog resolves its boto3 client's region from the
        # "glue.region" catalog property (falling back to the generic
        # "client.region", which the S3 FileIO also checks), never from
        # ambient EC2/ECS instance metadata; EMR Serverless's execution
        # environment doesn't populate that for boto3, so an unset region
        # here fails with "botocore.exceptions.NoRegionError: You must
        # specify a region" the moment the writer opens its Glue client (a
        # real, first-live-EMR-run finding, W2 sprint window, 2026-07-04).
        {
            "type": "glue",
            "warehouse": catalog_warehouse,
            "glue.region": aws_region,
            "client.region": aws_region,
        }
        if catalog_type == "glue"
        else {"type": "sql", "uri": catalog_type, "warehouse": catalog_warehouse}
    )
    spark = SparkSession.builder.appName("harbormaster-lake-backfill").getOrCreate()
    try:
        run(spark, raw_extract_s3_uri=raw_extract_s3_uri, catalog_props=catalog_props)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":  # pragma: no cover - EMR-only, no local JVM to run it against
    raise SystemExit(main(sys.argv))
