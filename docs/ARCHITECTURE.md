# Architecture

The design has a training plane on an off-cloud GPU cluster and a serving plane on AWS, joined by a model-promotion boundary. No trained checkpoint has been promoted into AWS serving, and the live endpoint served a labeled demo stand-in. This document covers the full topology and the tradeoffs behind it.

Status: the AWS demonstration environment is no longer running, and this repository deploys nothing by default. The Terraform modules describe what the bounded demonstration windows applied. See [HONESTY.md](HONESTY.md) for what was real, what was simulated, and what was never built. The five request and data paths are explained in plain language in [WORKFLOWS.md](WORKFLOWS.md), with diagrams in [`assets/diagrams/`](../assets/diagrams/).

## Hybrid topology

```
                   TRAINING PLANE  (off-cloud GPU cluster, Slurm)
  +-----------------------------------------------------------------------+
  |  Pi-DPM training jobs (design only, see the hpc/ job template)        |
  |  Output: versioned model artifacts (weights + metadata)               |
  +-----------------------------------------------------------------------+
                                   |
                                   |  model promotion (export -> S3 model bucket -> registry)
                                   v
  =======================================================================
                          SERVING PLANE  (AWS, no GPU training)
  =======================================================================

   Synthetic fixture replay  (live AIS is disabled in this snapshot)
        |
        v
   Fargate ingestor  (parse, normalize)
        |
        v
   Kinesis Data Streams  (durable ordered transport)
        |
        +------------------------------+
        v                              v
   Flink (Managed Flink)           Firehose
   per-vessel features, P_phys          |
        |                              v
        |                         S3 raw landing zone (JSON)
        |
        +-->  DynamoDB online feature table  (the scorer does not read it)
        |
        |  POST /v1/score-ais {mmsi, fix, history}
        v
   ECS Fargate: FastAPI scoring front door  <- - -> SageMaker async inference
   (gap, speed, and corridor-deviation              (client code only, and the ECS task
    detectors)                                       definition leaves HM_PIDPM_ENDPOINT unset)
        |
        v
   review queue (PostgreSQL hitl_queue)

   RDS (Postgres)  --Debezium on Kafka Connect-->  Kafka  -->  CDC worker
                   -->  DynamoDB read store, Redis cache, Iceberg audit table
                   (this repository claims an end-to-end pass on a local stack only)

   Observability:  CloudWatch metrics, a dashboard, and alarms  (serving plane)
   FinOps:         AWS Budgets  -->  soft budget (SNS alerts) | hard cap (deny policy on platform role)
```

## Plane responsibilities

**Training plane (off-cloud GPU cluster):** In the design, all GPU training happens here and produces the Pi-DPM diffusion model. The cluster runs Slurm, and jobs are submitted off-cloud. This repository holds only a job-manifest template in `hpc/` and no training code. Nothing in the training plane runs on AWS, which keeps GPU training costs off the AWS bill.

**Serving plane (AWS):** The serving plane handles everything live and low-latency. A Fargate ingestor normalizes each report and puts it on Kinesis, which is the durable transport. The ingestor keeps an AISStream live-feed parser as tested reference code, and this snapshot replays only a synthetic fixture. Two consumers read from Kinesis. Flink computes per-vessel features, writes them to a DynamoDB online table, and posts each gated report to the scoring service. Firehose lands the raw JSON in S3. The ECS Fargate scoring service runs the lightweight detectors inline and can call a SageMaker asynchronous endpoint, which is the target host for Pi-DPM. The ECS task definition in this snapshot does not set `HM_PIDPM_ENDPOINT`, so this repository claims no scorer-to-endpoint call on AWS. A temporary EKS and KEDA path was tested in one bounded window and then removed, so ECS remains the front door (see [the Production V1 front-door ADR](adr/ADR_PRODUCTION_V1_ECS_FRONT_DOOR.md)). Postgres plus Debezium on Kafka Connect provides CDC of operational registry state into the online stores and an Iceberg audit table. That path passed its checks on a local stack, and no end-to-end CDC run on AWS is claimed.

## Key tradeoffs

**Kinesis for the AIS stream:** Kinesis Data Streams carries the AIS reports. A managed stream priced per shard needs no broker operations and fits the cost cap. A provisioned Amazon MSK cluster would add always-on broker cost for this path, and the project could not justify that cost. Kinesis retention has a maximum of 365 days, which is acceptable in the design because the lakehouse would be the system of record. In this snapshot, Firehose lands raw JSON in S3, and the bridge into the lakehouse is assumed integration. The CDC path is the one exception, because Debezium runs on Kafka Connect and therefore uses Kafka.

**Flink over Spark Structured Streaming:** Streaming feature computation uses Flink through Managed Flink. Flink processes one event at a time, and its keyed state, timers, and event-time windows fit AIS, where late and out-of-order position reports are normal. Spark's micro-batch model adds latency and makes per-vessel event-time logic harder. The tradeoff is a steeper operational learning curve. The current job is a per-event keyed realization without watermarks, as [ADR 0001](adr/0001-streaming-per-event-realization.md) explains.

**Iceberg for the lakehouse:** The lake uses S3 plus Apache Iceberg. Iceberg gives schema evolution, hidden partitioning, snapshot isolation, and time travel. These features matter for a CDC audit sink and for reproducible training pulls. The tradeoff is extra catalog and maintenance work, such as snapshot expiry and compaction.

**No GPU training on AWS:** In the design, all GPU-bound training stays on the off-cloud cluster. GPU instances are the fastest way to exceed a small hard budget cap, and the off-cloud cluster already provides GPU capacity. SageMaker async inference is the target host for Pi-DPM, and its queue absorbs bursts so the endpoint needs no extra standing capacity. The demonstration endpoint served a labeled stand-in model on a GPU instance type (`ml.g4dn.xlarge`), so its scale-to-zero behavior mattered (war stories P25 and P26). The tradeoff is a model-promotion boundary to manage and higher inference latency for the heavy model.

**Bedrock for explanation only (built and tested, disabled, never deployed):** The explanation layer is a merged, tested module (`serving/app/bedrock_explainer.py`). It stays disabled unless `HM_BEDROCK_MODEL_ID` is set, and no serving route calls it. In the plan, Amazon Bedrock would write a plain-language explanation for an event that the detectors already flagged. In the design, the deterministic detectors and Pi-DPM would keep the detection decision. Bedrock would only describe a decision after it is made, so its text stays advisory.

## Cost and provider posture

- The FinOps module is provisioned first and gates everything else. It sets a soft budget with tiered SNS alerts and a hard cap whose budget action attaches an IAM deny policy to the platform role. The deny policy blocks new actions, so it is not a verified spending cutoff. A scheduled teardown function removes costly running resources, and war story P7 in [WAR_STORIES.md](WAR_STORIES.md) describes why the deny policy alone is not enough.
- Providers are pinned in `infra/terraform/versions.tf`, including `aws ~> 5.0`, `archive`, `random`, `helm`, and `tls`. War story P8 describes an anticipated case where an unpinned provider upgrade forces a resource replacement, and the pin prevents that case.
