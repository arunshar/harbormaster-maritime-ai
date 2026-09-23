# Honesty framing (locked)

This document is the single source of truth for how anyone describes Harbormaster, whether in the README, a blog post, a design note, or a commit message. It is locked. If another file in the repository disagrees with this framing, this file takes precedence, and the other file should be corrected.

## The locked statement

Harbormaster is personal work by Arun Sharma, and it contains no employer code, no employer data, and no employer infrastructure.

The project uses my own code, public AIS references, and a synthetic replay fixture. The [Reused code](../README.md#reused-code) section of the README lists the modules that come from three of my earlier repositories. The ingestor keeps an AISStream live-feed parser as tested reference code, and this snapshot disables live ingestion. The replay fixture in this snapshot is synthetic and contains no AISStream data. The public references in the design are the MarineCadastre historical AIS archive and NOAA Electronic Navigational Charts. A licensed historical AIS extract was used locally and is not redistributed.

## Current state

- The AWS demonstrations ran in bounded windows, and that demonstration environment is no longer running.
- This repository deploys nothing by default, and its Terraform modules describe what those demonstration windows applied.
- Candidate V3, a later Pi-DPM model candidate, is not deployed.
- Production V1 is not accepted, and the Production V1 ADR records a planning decision only.

## What Harbormaster shows, and what it does not

Harbormaster shows specific platform-engineering skills. This section lists those skills and the two capabilities that the project does not have.

**Shown:**

- Change data capture: The build uses Postgres logical decoding and Debezium on Kafka Connect with an LSN-guarded consumer, and it was tested on a local stack.
- Streaming: A Fargate ingestor, Kinesis, and a Managed Flink job ran in bounded AWS windows on a synthetic fixture replay.
- Distributed-systems practice: Several services with failure handling and backpressure ran in bounded demonstration windows. Production V1 is not accepted.
- MLOps: The build has a promotion state machine and Terraform for an async SageMaker endpoint. One bounded window exercised both with a labeled demo stand-in model and never a trained checkpoint.
- Observability: The AWS serving plane had CloudWatch metrics, a dashboard, and alarms.

**Not shown (out of scope and never claimed):**

- Harbormaster has no sharded query router, and it does not implement query sharding or a routing layer across shards.
- Harbormaster has no consensus implementation. It does not implement Raft, Paxos, or any consensus protocol, and it uses managed services for coordination.

If someone asks whether this project proves I can build a consensus system or a sharded query router, the honest answer is no.

## Real versus simulated: labeling rules

Most of Harbormaster is real engineering. The labeling rules below cover any simulated material, so the reader never has to guess which is which.

- **Real and unlabeled by default:** The infrastructure code, the streaming and CDC pipelines, the serving stack, the observability wiring, and the cost guardrails are real. The serving and streaming stacks ran in bounded AWS windows, and that environment is no longer running. The CDC worker was deployed on AWS, but no end-to-end CDC run on AWS is claimed.
- **Simulated and always labeled:** Any customer persona, client name, business case study, or "a customer asked us to..." narrative counts as simulated. Such an artifact must carry an explicit "SIMULATED" label at the top of its file or section. This public snapshot contains none.
- **Numbers:** Quote each figure exactly as it was measured, and label any estimate as an estimate.
- **Employer material:** The repository contains no employer client names, internal project names, private datasets, or architecture diagrams.

## Model evaluation boundary

A controlled benchmark of Candidate V3, a Pi-DPM model candidate, reported 99.1447% precision and 100% proxy recall under constructed labels. That benchmark did not score the detectors in this repository. These figures are not operational accuracy, because the benchmark constructed its own labels. A physics baseline also reached AUROC 1.0 on the same benchmark, so the benchmark shows no learned advantage for the model.

## Streaming and MLOps demonstrations on AWS (2026-07-04)

The MLOps work first ran only against fakes in tests. One bounded AWS window then ran it for real. The window ran a live EMR Serverless backfill, and its Great Expectations gate blocked a bad-data fixture with zero rows written. A live SageMaker async endpoint scaled from zero to one and from one to zero, which real CloudWatch alarm data confirmed. The window also created a real Model Package Group and ran the promotion state machine against the live endpoint. That endpoint had one production variant, so the run exercised weight updates but did not split traffic between two distinct models. The endpoint served a labeled `phase3-demo-standin` model throughout and never a real trained checkpoint. This section supports the infrastructure and promotion discipline, and it makes no claim about model quality. The Workflow 1 streaming plane (Kinesis, Flink, and the scoring service) also ran live for the first time in this window. A planted anomaly reached the review queue end to end.

## CDC status

The CDC pipeline is built and tested on a local stack. Postgres 16 logical decoding (pgoutput, explicit publication) feeds Debezium on Kafka Connect. An idempotent, LSN-guarded consumer then updates the DynamoDB and Redis online stores and appends to an Iceberg `cdc_audit` table. Replication-slot lag alerting and two drills cover the failure modes (war stories P9 and P10 in [WAR_STORIES.md](WAR_STORIES.md)). A later bounded AWS window deployed the Connect worker on managed Kafka and found the issues in war stories P42 to P46. This snapshot does not include evidence from any AWS CDC run after the P45 fix, so this repository claims no end-to-end CDC run on AWS.
