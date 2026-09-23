# System design decisions

This document maps each significant Harbormaster decision to an established distributed-systems pattern. It names the source that describes the pattern, states the reasoning and the tradeoff, and says what is built today.

Harbormaster is a personal maritime anomaly-detection project, and [HONESTY.md](HONESTY.md) explains what is real, what is simulated, and what was never built. The design ingests AIS vessel reports, serves spatial anomaly detectors over a streaming feature plane on AWS, and plans to train heavy models on an off-cloud GPU cluster. The AWS environment is no longer running, and no trained checkpoint was deployed. The honesty rule applies to this document too, so measured figures are quoted as measured and estimates are labeled as estimates.

## Build status

Several decisions are only partly built. Read each record with this map in mind.

- **Built and tested locally:** the CDC path uses Debezium on Kafka Connect and Kafka, and an LSN-guarded consumer applies each change (DR-1, DR-2). The Iceberg `cdc_audit` table (DR-5), the P_phys cheap gate (DR-6), tenant row-level security (DR-7), the promotion state machine (DR-3), and the burn-rate calculator (DR-13) are also in the code.
- **Run in bounded AWS windows:** the promotion state machine ran against a live SageMaker async endpoint with one production variant (DR-3). That run exercised weight updates but did not split traffic between two models.
- **Partly built:** the Flink job is a per-event keyed realization without watermarks ([ADR 0001](adr/0001-streaming-per-event-realization.md)). The async model client has a timeout and falls back to an analytic score, but it has no circuit breaker or jittered retry (DR-8). Tenant SLO tiers and tenant drift checks exist, but per-tenant resource pools and per-tenant models do not (DR-7).
- **Design only:** an Argo CD rollout and the drift-to-retrain loop (DR-3), a CDC schema registry (DR-10), and API idempotency keys and cursor pagination (DR-15) do not exist in this repository.

## How to read a record

A pattern is a named, reusable answer to a recurring design problem, together with the context where it applies and the tradeoff it imposes. Each decision record below has the same parts:

- **Decision:** This part states the choice Harbormaster makes. A label says when the decision describes the target design.
- **Pattern and source:** This part names the pattern and a source that describes it.
- **Reasoning:** This part explains why the choice fits and which failure it prevents.
- **Tradeoffs:** This part states what the choice gives up and which alternatives the project rejected.
- **Built today:** This part states what this repository actually contains.

Public articles are cited with their URL. Books are cited by chapter or pattern name, and this document quotes none of them.

---

## Decision records

### DR-1: CDC pipeline (RDS Postgres logical decoding to derived stores)

**Decision (target design):** Operational state lives in RDS Postgres. Logical decoding and Debezium capture each committed change, and Kafka Connect publishes it to Kafka as a change stream. An idempotent, LSN-guarded consumer applies each change to the derived stores. DynamoDB holds the online read projection, Redis holds a hot cache, and an Iceberg `cdc_audit` table holds the append-only history.

**Pattern and source:** The patterns are Change Data Capture, the dual-write problem, and log-based message brokers. Kleppmann covers change data capture and log-based brokers in chapter 11 of *Designing Data-Intensive Applications*, and he covers derived data and unbundling the database in chapter 12. Chris Richardson's Transactional Outbox page on microservices.io (https://microservices.io/patterns/data/transactional-outbox.html) gives the dual-write framing.

**Reasoning:** A naive design writes to Postgres and then writes to the cache and the feature store from application code. That is a dual write, because two systems change in two separate operations with no shared transaction. A crash between the two writes leaves the stores inconsistent, and no simple retry fixes it, because the caller does not know which write landed. CDC removes the dual write. The application writes once, to Postgres, and every derived store becomes a deterministic function of the ordered change log. The change log is the contract, and each derived store is a projection of that log. Kleppmann calls this idea unbundling the database.

**Tradeoffs:** The main alternative is the Transactional Outbox, in which the business row and an outbox row commit in one local transaction and a relay publishes the outbox. The outbox fits when you control the application's write path and want application-level events. Harbormaster chose log-based CDC because it captures every change to operational state with no application changes, and no developer can forget to write an outbox row. The costs are operational. Logical decoding adds load on Postgres, the connector must run somewhere, and schema changes need discipline. CDC also imposes eventual consistency between the write model and the read models (DR-4, DR-16) and a cold-start snapshot cost (war story P3).

**Built today:** The schema and the connector config set up logical decoding (pgoutput) with an explicit publication. Debezium publishes to Kafka through Kafka Connect, and the consumer writes DynamoDB, invalidates Redis, and appends to Iceberg `cdc_audit`. The code lives under `cdc/`, and the whole path is tested on a local stack. The Connect worker was deployed on AWS, but no end-to-end CDC run on AWS is claimed.

### DR-2: Idempotent consumer (LSN-guarded upsert)

**Decision:** The CDC consumer is idempotent. It upserts by primary key and guards each write with a monotonic `last_applied_lsn`, so it skips any record whose LSN does not exceed the one already applied. It commits the Kafka offset only after every sink acknowledges the write, and it represents deletes as guarded soft-delete markers.

**Pattern and source:** The patterns are Idempotent Receiver, High-Water Mark, and Versioned Value. Joshi describes all three in *Patterns of Distributed Systems*, and his Idempotent Receiver article is at https://martinfowler.com/articles/patterns-of-distributed-systems/idempotent-receiver.html. Kleppmann covers effectively-once processing through idempotence in chapter 11.

**Reasoning:** Kafka, like any log-based broker, gives at-least-once delivery. After a consumer restart or a rebalance, the consumer re-reads records it already processed. True exactly-once delivery across a network and an external sink is not achievable in general, so the realistic target is effectively-once state. At-least-once delivery plus idempotent application reaches the same end state however many times a record is redelivered. The upsert by primary key turns a re-applied insert into a no-op. The monotonic LSN guard rejects out-of-order and stale redeliveries, so it acts as a high-water mark over the WAL's log sequence numbers. Committing the offset only after the sink acknowledges closes the last gap. If the consumer committed first and then crashed, the record would never return, and a crash before the commit simply leaves the record to be read again.

**Tradeoffs:** Broker transactions or two-phase commit to the sink add coordination cost, and in practice they still reduce to idempotent writes plus deduplication. Effectively-once processing keeps the consumer simple and fast. The cost is that every sink write must be idempotent, and the high-water mark must be stored durably next to the sink. Delete markers cost extra storage, and downstream readers must honor them.

**Built today:** The consumer stores `last_applied_lsn` for each key in the sink, and it applies a record only when the record's LSN is greater than the stored value (`cdc/sinks/dynamo.py`). It then writes to DynamoDB and Redis, appends the change to Iceberg, and commits the Kafka offsets after every sink acknowledges the write (`cdc/consumer/applier.py`). Cache invalidations and audit rows can repeat after a redelivery, so the system does not claim exactly-once effects across every destination.

### DR-3: Model promotion and the retraining loop

**Decision (target design):** A model trained on the off-cloud GPU cluster moves through a multi-stage gate. The gate starts with a holdout quality check and then runs shadow, a parallel run with no production effect. Canary traffic then ramps through 5%, 25%, and 50%, and a full rollout through Argo CD follows. The design also includes a retraining loop, and that loop does not run in this repository. In the design, drift detection sends cases to human review, and reviewed preferences feed preference-based retraining. Today, analyst feedback is stored for later comparison and does not retrain the model.

**Pattern and source:** The patterns are sagas with orchestration or choreography, compensating transactions, parallel run, and canary release. Chris Richardson describes the Saga pattern on microservices.io (https://microservices.io/patterns/data/saga.html). Sam Newman covers sagas and deployment patterns in *Building Microservices*, 2nd edition. Martin Fowler describes Canary Release (https://martinfowler.com/bliki/CanaryRelease.html) and Blue-Green Deployment (https://martinfowler.com/bliki/BlueGreenDeployment.html). The saga section below covers this record in more depth.

**Reasoning:** Promotion is a multi-step process in which each step can fail and some steps have side effects that must be undone. A saga is a sequence of local steps, and each step has a compensating action. The steps are coordinated so that a failure triggers the compensations for the steps already taken. The compensation for moving canary traffic to a new model is moving it back, which is the auto-rollback. Modeling promotion as a saga makes auto-rollback a tested, ordinary path.

**Tradeoffs:** Orchestration uses a central coordinator, and choreography lets services react to events. Harbormaster uses orchestration for promotion because the sequence is fixed and the rollback logic must be auditable. Canary exposes a small share of traffic first and then ramps up, which catches quality regressions that depend on traffic. Blue-green flips all traffic at once between two identical environments, so it gives fast rollback but no graduated exposure. Harbormaster reserves blue-green for stateless infrastructure swaps. The saga approach needs an orchestrator, and every step and every compensation must be safe to run more than once.

**Built today:** `mlops/promote.py` is a callback-injected policy state machine. It runs a holdout gate, a reward-hacking probe, a shadow step, and canary weights of 5, 25, 50, and 100. `mlops/shadow_diff.py` compares two already-collected score arrays locally, and it shifts no traffic. One bounded AWS window ran the state machine against a live SageMaker async endpoint with one production variant. The run exercised weight updates and did not split traffic between two models, and the endpoint served a labeled demo stand-in model. No deployed weighted canary exists, Argo CD is not used, and the retraining loop has not run.

**Forward note:** SageMaker deployment guardrails support asynchronous endpoints, and the project considered them. Any future rollback decision must use the approved burn-rate rule for the SLO error budget. It must not use a raw alarm as an unreviewed stand-in. A real candidate comparison would first need a reviewed dual-endpoint or real-time endpoint design that maps that burn-rate rule to its traffic-control mechanism.

### DR-4: Read models and the write model (CQRS)

**Decision (target design):** Postgres holds the write model, which is the registry and the operational state. DynamoDB and a Redis cache hold the read models. Writes go to Postgres, serving reads go to the read models, and the CDC pipeline (DR-1) connects the two.

**Pattern and source:** The patterns are CQRS and materialized views. Fowler describes CQRS at https://martinfowler.com/bliki/CQRS.html, and Richardson describes it on microservices.io (https://microservices.io/patterns/data/cqrs.html). Kleppmann covers read-your-writes consistency in chapter 5.

**Reasoning:** The write side and the read side have different shapes. The write side needs transactional integrity, relational constraints, and a normalized registry, and Postgres fits that. The read side needs fast point lookups per vessel at serving time, and a denormalized key-value store fits that better. CQRS makes the split explicit, so each side can be tuned on its own. The read model is then a derived, eventually consistent projection of the write model.

**Tradeoffs:** One store for both paths is simpler and strongly consistent, but it forces a compromise schema and ties serving latency to the load on the write store. The main risk of CQRS is read-your-writes. A client that just wrote to Postgres may not see the change in the read model yet. Harbormaster accepts this risk because registry context tolerates a short replication delay, and slot-lag alerting makes that delay visible (ADR 0002). In the design, only the metadata of a newly promoted model needs fresh data, so that case would read the registry directly. CQRS adds too much complexity for a simple CRUD app. Harbormaster uses it because its read and write workloads differ.

**Built today:** The scorer reads the CDC-fed registry projection in DynamoDB and Redis, and it does not read the feature store. The registry routes under `/v1/registry/...` write the registry rows.

### DR-5: Iceberg audit log (event sourcing)

**Decision (target design):** The Iceberg `cdc_audit` table is append-only. It records the full ordered history of changes and not just the current state. In the design, the anomaly event stream is also a log of detected events, and derived state comes from these logs.

**Pattern and source:** The patterns are Event Sourcing and the log as the source of truth. Fowler describes Event Sourcing at https://martinfowler.com/eaaDev/EventSourcing.html, and Kleppmann covers the log as the source of truth in chapters 11 and 12.

**Reasoning:** A store that keeps only current state loses its history. It cannot answer what the system believed at a given time, and it cannot rebuild a derived store after a bug. An append-only log keeps every change as an immutable fact, so current state is a fold over the log and any derived view can be rebuilt by replay. An anomaly platform needs that history for audits and for reproducible training pulls (DR-12). Iceberg's snapshot isolation and time travel make the log queryable at a point in time.

**Tradeoffs:** Mutable current-state tables are smaller and simpler, but they cannot be audited or replayed. Event sourcing costs storage and adds compaction and snapshot-expiry work (war story P6). It also pushes work onto readers, who must fold the log or query the right snapshot. Harbormaster applies event sourcing only to the audit and anomaly logs, and the registry stays as current state in Postgres.

**Built today:** Firehose lands raw JSON in S3, and the CDC worker appends to the Iceberg `cdc_audit` table with each change's LSN. `lake/iceberg.py` has a tested maintenance routine that compacts data files and expires old snapshots. The bridge from Firehose JSON to backfill Parquet is assumed integration.

### DR-6: Streaming ingestion with Flink and the P_phys cheap gate

**Decision (target design):** Flink computes streaming features with event-time windows and watermarks, and backpressure propagates when a downstream stage is slow. A cheap physics-based gate, `P_phys`, runs first. It drops reports whose required speed is more than about 3.3 times the 25-knot cap, and it sends every other report to the scorer.

**Pattern and source:** Kleppmann covers backpressure, flow control, watermarks, and event-time windows in chapter 11. Nygard describes Load Shedding and Steady State in *Release It!*.

**Reasoning:** A streaming system has a producer, which is the AIS feed at its own rate, and a consumer that computes features and calls models. The consumer can fall behind. Without flow control, a slow consumer either drops data or runs out of memory while it buffers. Backpressure slows the producer so that the system stays up, and Flink provides it through bounded buffers. Load shedding covers the case where backpressure is not enough, and the careful choice is to drop the cheapest, least valuable work first. In the current code, `P_phys` drops only reports that need more than about 83 knots, so every plausible report still reaches the scorer. The gate does not reduce the load from normal traffic.

**Tradeoffs:** An unbounded queue only postpones the problem, and silent drops lose data. Backpressure trades throughput under overload for stability. Calling the heavy model on every event is simpler, but it spends the expensive path on reports that are clearly normal. The current gate does not avoid that cost, because it passes every plausible report. `P_phys` carries a false-negative risk. Reports that need between about 83 and 300 knots are dropped, although the scorer would flag them as implausible speed. Watermarks carry their own risk, because adversarial timestamps can stall them (war story P2).

**Built today:** The Flink job keys state per vessel and applies `P_phys` inline, and only reports that pass the gate reach the scorer. The job has no event-time windows or watermarks ([ADR 0001](adr/0001-streaming-per-event-realization.md)).

### DR-7: Multi-tenant isolation (bulkhead and default-deny)

**Decision (target design):** Every record and request carries a `tenant_id`, and access is default-deny, so a request with no matching tenant grant is rejected. In the design, tenants also get separate resource pools and separate models, so one tenant's load or failure cannot starve another.

**Pattern and source:** The main pattern is the Bulkhead, which Nygard describes in *Release It!* and Newman covers in *Building Microservices*, 2nd edition. Cell-based architecture and shuffle sharding generalize the same isolation idea. Default-deny is a standard security posture.

**Reasoning:** A bulkhead contains a failure so that it cannot spread through the whole system. Without isolation, one noisy or compromised tenant can use up a shared pool of connections, threads, or model capacity and degrade every tenant. Separate pools stop the damage at the partition boundary. Default-deny is the security counterpart. When a tenant grant is missing or ambiguous, the safe default is to refuse, because a fail-open default leaks data across tenants.

**Tradeoffs:** A shared pool is cheaper and gives better average utilization. The bulkhead gives up some utilization, because it reserves idle capacity for each tenant, and it gains guaranteed isolation. A separate stack per tenant is the strongest form and the most expensive. Default-deny costs occasional friction, because a new tenant is refused until someone grants access.

**Built today:** `tenant_id`, row-level security, composite keys, SLO tiers, and per-tenant drift checks exist (war stories P35, P36, and P39). Per-tenant pools and per-tenant models do not exist.

### DR-8: Async call to the heavy model

**Decision (target design):** The scorer calls the heavy model, Pi-DPM on a SageMaker asynchronous endpoint, with a timeout and a circuit breaker. In the design, bounded retries use exponential backoff with jitter, and "no result within the SLA" counts as an explicit failure.

**Pattern and source:** Fowler describes the Circuit Breaker at https://martinfowler.com/bliki/CircuitBreaker.html, and Nygard covers it in *Release It!*. Nygard and Newman (*Building Microservices*, 2nd edition) both cover timeouts and retries.

**Reasoning:** Any call across a process boundary can hang, and a hung call holds a resource such as a thread or a slot. Under load, many hung calls exhaust the caller, and one slow dependency turns into a full outage. A timeout bounds the wait. A circuit breaker opens after a threshold of failures and fails fast for a cooldown period, so the caller stops sending work to a failing dependency. It then half-opens to test recovery. Retries handle short transient failures, but naive retries all arrive at once when the dependency recovers, and exponential backoff with jitter spreads them out. An asynchronous endpoint is also a queue with a drop policy (war story P4), so a missing result must surface as an observable failure.

**Tradeoffs:** Synchronous real-time inference gives lower latency per call, but it cannot absorb bursts and needs extra standing capacity to avoid drops. The async endpoint absorbs bursts in its queue, and its latency is higher and more variable. That is acceptable because detection is not real-time-critical. The cheap gate (DR-6) does not reduce this load, because it drops only reports that need more than about 83 knots. An over-eager breaker can reject healthy traffic, so its threshold and cooldown need tuning.

**Built today:** The async client in `serving/app/pidpm_client.py` has a timeout and an analytic fallback, so an outage lowers quality and scoring continues. It has no circuit breaker and no jittered retry.

### DR-9: Stream partition design

**Decision:** The partition key for the AIS stream is the MMSI, which has high cardinality. A coarse region code would concentrate traffic. Shard count follows throughput, and in the design a per-shard hot-key metric makes skew visible before it throttles.

**Pattern and source:** Kleppmann covers partitioning, skew, and hot spots in chapter 6, and he covers consumer-group rebalancing in chapter 11. War story P1 describes the anticipated hot-shard case.

**Reasoning:** A partitioned log spreads load by key, so the key choice decides whether the load is even. A low-cardinality or skewed key, such as a region code where a few dense lanes carry most traffic, puts too much load on one shard. That shard throttles while its siblings stay idle. Adding shards does not help, because the key distribution causes the skew and the total throughput does not. A high-cardinality key such as the MMSI spreads per-vessel traffic evenly and keeps each vessel's reports in order on one shard. Graph the throughput of each shard as well as the total, because a healthy total can hide one hot shard.

**Tradeoffs:** Keying by vessel keeps per-vessel ordering and keyed state, which feature computation needs. Explicit hot-key splitting, which salts the hottest keys, is more complex and suits a case where one key dominates. Rebalancing during scale events briefly disrupts consumers, and idempotent processing (DR-2) handles that.

**Built today:** The ingestor uses the MMSI as the Kinesis partition key (`streaming/ingestor/ingest.py`). The hot-key metric is design only.

### DR-10: CDC event schema (encoding and evolution)

**Decision (target design):** CDC events would use an explicit, versioned schema in a schema registry. The registry would enforce backward and forward compatibility, so producers and consumers could deploy independently.

**Pattern and source:** Kleppmann covers encoding and evolution, schema registries, and compatibility in chapter 4.

**Reasoning:** In a streaming system, producers and consumers deploy at different times and run different code versions at once. The schema must therefore evolve without a flag day on which everything redeploys together. Backward compatibility means new consumer code can read old data, so the consumer can deploy first. Forward compatibility means old consumer code can read new data, so a producer can add a field before every consumer knows about it. Honoring both limits schema changes to safe operations, such as adding optional fields. A schema registry enforces this at publish time, so an incompatible change fails at deploy time and not as a deserialization failure in production.

**Tradeoffs:** Schemaless JSON is easy to start with, but it pushes every compatibility decision to runtime. A registry plus a binary encoding such as Avro or Protobuf costs setup work and a registry to operate. In exchange it gives publish-time guarantees and a compact wire format. The constraint on developers is real, because only additive evolution is allowed.

**Built today:** This record is not built. The consumer parses Debezium JSON envelopes in `cdc/consumer/`.

### DR-11: Growing the platform phase by phase (strangler fig)

**Decision:** Harbormaster grows incrementally and avoids a big-bang cutover. It stands up streaming, CDC, serving, and observability as separate phases, and each new piece is verified before the next one starts.

**Pattern and source:** The pattern is the Strangler Fig Application, which Fowler describes at https://martinfowler.com/bliki/StranglerFigApplication.html.

**Reasoning:** A big-bang rewrite carries all of its risk until the single cutover, and a failed cutover delivers no value and forces a hard rollback. The strangler fig approach builds the new system alongside the old one and moves one capability at a time. Each phase delivers standalone value and can be reversed on its own, so risk goes down continuously.

**Tradeoffs:** A rewrite is simpler to reason about and avoids running two systems at once, but its risk profile is poor. The strangler fig costs a longer timeline and an interim state in which old and new parts coexist. Harbormaster accepts the longer timeline because incremental, reversible delivery suits a personal learning platform. The cost cap also makes it sensible to build the cheapest safe piece first.

**Built today:** The build followed this order. Foundations and FinOps came first, and streaming, serving, the lake and CDC, and observability followed as separate phases.

### DR-12: Batch backfill, live stream, and replay (lambda and kappa)

**Decision:** Harbormaster runs a live stream (Flink) for current features and a batch path (EMR Serverless) for historical backfill. The design leans toward kappa, which reprocesses by replaying the log, and it keeps a batch backfill where replaying a large archive through the stream would be wasteful.

**Pattern and source:** The patterns are the Lambda and Kappa architectures. Nathan Marz coined the Lambda architecture, and Jay Kreps described Kappa in "Questioning the Lambda Architecture" (O'Reilly Radar, 2014). Kleppmann treats both in chapters 11 and 12.

**Reasoning:** The Lambda architecture runs two code paths, a batch layer and a speed layer, and merges their results. Its known cost is keeping the same logic in two engines and reconciling their outputs. Kappa removes that cost. When the log is the source of truth and can be replayed, reprocessing means replaying it through the same streaming code. A large historical backfill is the practical exception, because replaying it through the live stream would compete with live traffic and run slowly. A bounded EMR batch job handles the one-time cold load.

**Tradeoffs:** Pure Lambda keeps a proven batch layer, but it duplicates logic and lets the two paths drift apart. Pure Kappa is cleaner, but it assumes that the log keeps enough history and that replay throughput is acceptable, and neither holds for a large cold start. The hybrid runs two engines, Flink and EMR, and that is justified because they do different jobs.

**Built today:** Flink computes live features, and an EMR Serverless backfill ran in one bounded AWS window (war stories P17 to P20). Replay-based reprocessing from Iceberg snapshots is design only.

### DR-13: SLOs, error budgets, and burn-rate rollback

**Decision (target design):** Serving has explicit SLOs for latency and availability. The complement of each SLO is an error budget, and burn rate drives alerting and canary auto-rollback. Burn rate measures how fast the current error rate consumes the budget.

**Pattern and source:** The patterns are SLOs, error budgets, and burn-rate alerting. The Google SRE books (https://sre.google/books) cover them in the chapters on service level objectives and on alerting on SLOs. The auto-rollback link is the compensating transaction of the promotion saga (DR-3).

**Reasoning:** A statement about a service's health needs a number, and an SLO provides it. The error budget makes the SLO actionable. For example, a 99.9% availability target leaves a 0.1% error budget, which turns reliability into a measurable limit on risk. Burn-rate alerting pages on how fast the budget is being spent, and it ignores the raw error count. A fast burn pages at once, and a slow burn becomes a ticket. Wiring canary rollback to the burn rate makes the rollback decision objective and automatic.

**Tradeoffs:** Threshold alerting is simpler, but it either pages on harmless short spikes or misses slow degradation. Burn-rate alerting needs fast and slow windows tuned correctly, and it gives a much better signal. An error budget also needs discipline, because risky changes must stop when the budget runs out.

**Built today:** A burn-rate calculator exists in `serving/app/burn_rate.py`, and the observability module defines fast and slow burn-rate alarms. The local promotion state machine reverts on a burn-rate breach. No deployed canary exists.

### DR-14: Capacity sizing for a live AIS feed

**Decision:** Shard count, storage growth, and rough cost should come from a back-of-envelope estimate of the AIS message rate and not from a guess. The method section below shows the steps.

**Pattern and source:** The method is back-of-envelope estimation from stated assumptions. Kinesis sizing follows from the partitioning analysis in DR-9.

**Reasoning:** An estimate turns "it should scale" into a number that others can check. State the assumptions, derive one rate, and size each resource from that rate. Doing this before provisioning prevents throttling from under-provisioning and waste from over-provisioning.

**Tradeoffs:** An estimate depends on assumptions that may be wrong. Stating them lets others challenge and re-run the estimate, and sizing with headroom covers bursts.

**Built today:** No live feed runs in this snapshot, so this record describes the method only.

### DR-15: Serving API design

**Decision (target design):** In the target design, write endpoints accept idempotency keys, list endpoints use cursor-based pagination, and the API is versioned and resource-oriented.

**Pattern and source:** Idempotency keys generalize the Idempotent Receiver (DR-2). Joshi describes the Request Pipeline pattern in *Patterns of Distributed Systems*.

**Reasoning:** Clients retry over a network, so a non-idempotent write is sent twice when a client times out and retries. An idempotency key lets the server deduplicate and return the original result, which applies the principle of DR-2 at the API boundary. Cursor pagination stays correct on a live, changing dataset, because offset pagination skips or repeats rows when the data shifts between pages. Explicit versioning lets the API evolve without breaking existing clients.

**Tradeoffs:** Offset pagination is simpler and allows jumps to any page, but it is wrong for a live feed. Idempotency keys require the server to store seen keys for a window. Versioning requires support for old versions during a deprecation window. These choices are standard and have little controversy.

**Built today:** This record is not built. The implemented routes are `POST /v1/score-ais`, `POST /v1/feedback`, `GET /v1/hitl/pending`, and the `/v1/registry/...` routes for vessels, watchlist entries, and sanctions flags. They are versioned under `/v1`, and none of them accepts an idempotency key or uses cursors. A promotion route such as `POST /v1/models/{id}/promote` is also design only.

### DR-16: Consistency between the Postgres write and the read-model read (PACELC)

**Decision:** Harbormaster accepts eventual consistency between the Postgres write model and the read models on the hot serving path. In the design, strong read-your-writes consistency is reserved for the few operations that need it.

**Pattern and source:** The topics are PACELC, eventual and strong consistency, read-your-writes, linearizability, and quorums. Kleppmann covers these consistency models, read-your-writes, and quorums in chapters 5 and 9.

**Reasoning:** PACELC extends CAP with the everyday case. During a network partition, a system trades availability against consistency. In normal operation, it trades latency against consistency, and that second choice matters on most days. Strong consistency costs latency, because every read must coordinate. Eventual consistency gives lower latency, and reads can be stale. The hot path reads registry context that tolerates a short replication delay, so eventual consistency on the CDC-fed read model is the right default. The careful step is to name the few operations that need read-your-writes and to route them to the write store.

**Tradeoffs:** Strong consistency everywhere would simplify reasoning, but it would add coordination latency to every serving read and tie read availability to the write store. Eventual consistency everywhere risks a bug in which a control-plane action is not yet visible. The choice is made per operation, which is the point of PACELC.

**Built today:** The scorer reads the eventually consistent DynamoDB and Redis projection, and slot-lag alerting monitors the CDC lag behind it.

---

## The promotion saga in more depth

This section expands DR-3. Its sources are Chris Richardson's Saga pattern (https://microservices.io/patterns/data/saga.html) and Sam Newman's *Building Microservices*, 2nd edition, on sagas, orchestration, and choreography.

### Why not a distributed transaction

The textbook way to make "promote the model, shift traffic, update the registry, and notify" atomic is a distributed transaction with two-phase commit across all the services. Two-phase commit is not available across different managed services such as SageMaker, the registry, and traffic routing, and it would not be desirable. It blocks while the coordinator holds locks across services, and it handles a coordinator crash poorly. These services also share no common transaction protocol. A saga makes a multi-step process reliable when no single ACID transaction can span all the steps.

### A saga is local steps plus compensations

A saga is a sequence of local transactions, and each step has a compensating transaction that undoes its effect. A saga has no global rollback. If step N fails, the saga runs the compensations for the earlier steps in reverse order. The promotion saga looks like this:

| Forward step | Compensating action |
| --- | --- |
| Pass the holdout quality gate | None, because the step has no external effect and a failure aborts before any traffic moves |
| Deploy the candidate in shadow | Tear down the shadow deployment |
| Canary 5% of traffic | Revert all traffic to the incumbent in one step |
| Canary 25% | Revert all traffic to the incumbent in one step |
| Canary 50% | Revert all traffic to the incumbent in one step |
| Full rollout | Roll back to the incumbent revision |

Auto-rollback is the compensating transaction for whichever step the burn-rate breach (DR-13) interrupts. Each step's compensation is defined up front, so rollback is a tested, ordinary path. The local state machine in `mlops/promote.py` reverts to the champion in one step at any canary weight (war story P12).

### Orchestration and choreography

In choreography, each service emits events and the next service reacts, and no central coordinator exists. In orchestration, a central coordinator invokes each step and decides what to compensate on failure. Choreography is loosely coupled, but it scatters the workflow logic across services, so no single place shows where a saga stopped and why. Orchestration centralizes the logic, so the workflow is easier to reason about and audit. The cost is that every step depends on the coordinator.

Harbormaster uses orchestration for promotion, because the sequence is fixed and the reason for each rollback must be auditable. In the design, the drift-to-retrain loop is more event-driven. It would lean toward choreography at the outer level, while promotion stays orchestrated.

### Why durable execution fits the design

The promotion saga would run for a long time and include a human approval that can take hours or days. The retraining loop would run even longer. An orchestrator that holds the workflow in memory loses everything if it restarts in the middle of a saga. Durable execution engines, such as Temporal, persist the workflow's state and history. They survive restarts, retry each step with its own policy, and wait on a human signal without holding resources. Harbormaster does not run a durable execution engine, and `mlops/promote.py` is an in-process state machine.

### Every step must be idempotent

Every saga step and every compensation must be idempotent, because a durable engine retries steps after crashes and may deliver signals twice. Shifting traffic to 25% twice must equal doing it once, and tearing down the shadow twice must not fail on the second call. This is the same effectively-once discipline as DR-2, applied to control-plane operations. Without idempotent steps, each retry repeats its side effects.

---

## Capacity sizing method for a live AIS feed

This section shows the DR-14 estimation method with symbols, and it uses no measured figures.

- Let V be the number of vessels that transmit in a busy window, r the mean report rate per vessel, and b the bytes in one normalized report.
- The mean message rate is R = V × r, and a burst factor k gives a peak rate of k × R.
- A Kinesis shard ingests up to 1,000 records per second or 1 MB per second, whichever limit binds first. The shard count is the larger of kR / 1,000 and kRb / 1 MB, plus headroom.
- Raw storage per day is R × b × 86,400 bytes, and a columnar format such as Parquet in Iceberg compresses it further.
- Shard-hours dominate the Kinesis cost, so the shard count sets the cost of a continuous full feed.

That shard cost is why the personal build ran reduced, intermittent ingestion in bounded windows and relied on the teardown Lambda (war story P7). The design sizes to a full feed, and the personal demonstrations ran a bounded slice within the cost cap.

---

## Design summary

**Requirements:** The system must ingest AIS reports, compute per-vessel streaming features, and detect anomalies. Light spatial detectors run inline, and a heavy diffusion model runs asynchronously. The system must also serve anomaly events for review, support multiple tenants, and keep an auditable history. The non-functional requirements that drive the design are event-time correctness under late and out-of-order AIS, low-latency serving reads, effectively-once processing, tenant isolation, reproducible training data, and a hard cost ceiling. The explicit non-goals come from [HONESTY.md](HONESTY.md). Harbormaster has no sharded query router and no consensus protocol, and it uses managed services for coordination.

**Core entities:** The core entities are the vessel (keyed by MMSI), the position report (the AIS event), the per-vessel feature, the anomaly event with its evidence, the versioned model, and the tenant. They map to the streams, the online store, the registry, and the audit log.

**API:** The implemented routes include `POST /v1/score-ais`, `POST /v1/feedback`, `GET /v1/hitl/pending`, and the `/v1/registry/...` routes for vessels, watchlist entries, and sanctions flags. DR-15 adds idempotency keys, cursor pagination, and a promotion route as design only.

**High-level design:** The serving plane follows [ARCHITECTURE.md](ARCHITECTURE.md). A Fargate ingestor puts reports on Kinesis, keyed by MMSI, and this snapshot replays only a synthetic fixture. Two consumers read from Kinesis. Flink writes features to DynamoDB and posts gated reports to the scorer, and Firehose lands raw JSON in S3. The ECS Fargate scoring service runs the light detectors inline and has a client for a SageMaker async endpoint for Pi-DPM. The ECS task definition leaves that client disabled. Postgres plus Debezium on Kafka Connect feeds the online stores and the audit table, and this repository claims an end-to-end run of that path on a local stack only. In the design, the training plane runs on an off-cloud GPU cluster, and models promote across the boundary. The main patterns are CDC, CQRS, event sourcing in the audit log, and a guarded async call to the heavy model.

**Topics that span several records:**

- Hot-shard partitioning (DR-9, war story P1) explains why the MMSI is a better key than a region code and why per-shard metrics matter.
- Effectively-once CDC (DR-1, DR-2, war story P10) covers the dual-write problem, the LSN high-water-mark guard, and the offset commit after the sink acknowledgement.
- The promotion saga (DR-3, the saga section, and war story P12) covers orchestration, choreography, compensations, and durable execution.
- Event-time watermarks and backpressure (DR-6, war story P2) explain why windows stall and why the current job has no watermark yet. They also explain why the P_phys gate drops only implausible reports and sheds no normal load.
- Consistency (DR-16) covers PACELC, why the hot path is eventual, and which operations need read-your-writes.

---

## Patterns catalog

| Pattern | Source | Where in Harbormaster |
| --- | --- | --- |
| Change Data Capture | Kleppmann, chapter 11; Richardson, microservices.io | RDS to Debezium on Kafka Connect to Kafka to derived stores (DR-1) |
| Transactional Outbox and dual writes | Richardson, microservices.io | The problem CDC avoids (DR-1) |
| Idempotent Receiver | Joshi, *Patterns of Distributed Systems* | LSN-guarded upsert consumer (DR-2) |
| High-Water Mark and Versioned Value | Joshi, *Patterns of Distributed Systems* | `last_applied_lsn` guard (DR-2) |
| Saga (orchestration and choreography) | Richardson; Newman, *Building Microservices* | Promotion state machine, with the retraining loop as design only (DR-3) |
| Compensating transaction | Richardson and Newman | Canary auto-rollback (DR-3, DR-13) |
| Durable execution and workflow engines | Newman | Design only (DR-3) |
| Parallel run and shadowing | Newman, *Building Microservices* | Local paired-score comparison before canary (DR-3) |
| Canary Release | Fowler, CanaryRelease | Canary weights in the local state machine (DR-3) |
| Blue-Green Deployment | Fowler, BlueGreenDeployment | Considered and reserved for infrastructure swaps (DR-3) |
| CQRS | Fowler, CQRS; Richardson | Postgres writes and DynamoDB and Redis reads (DR-4) |
| Event Sourcing | Fowler, EventSourcing; Kleppmann, chapters 11 and 12 | Iceberg `cdc_audit` append-only log (DR-5) |
| Unbundling the database | Kleppmann, chapter 12 | The change log connects the store and the cache (DR-1, DR-5) |
| Backpressure and flow control | Kleppmann, chapter 11 | Flink bounded buffers (DR-6) |
| Load Shedding and Steady State | Nygard, *Release It!* | P_phys cheap gate, which drops only implausible reports (DR-6) |
| Bulkhead | Nygard, *Release It!*; Newman | Per-tenant pools and models, design only (DR-7) |
| Default-deny and fail-closed | Security practice | Tenant row-level security (DR-7) |
| Circuit Breaker | Fowler, CircuitBreaker; Nygard | Async Pi-DPM call, design only (DR-8) |
| Timeout and retry with backoff and jitter | Nygard, *Release It!*; Newman | Timeout built, jittered retry design only (DR-8) |
| Partitioning, skew, and hot spots | Kleppmann, chapter 6 | MMSI partition key (DR-9, war story P1) |
| Encoding, evolution, and schema registries | Kleppmann, chapter 4 | CDC event schema, design only (DR-10) |
| Strangler Fig | Fowler, StranglerFigApplication | Phased platform build (DR-11) |
| Lambda and Kappa | Marz (Lambda); Kreps (Kappa); Kleppmann, chapters 11 and 12 | Flink, EMR, and replay design (DR-12) |
| SLOs, error budgets, and burn rate | Google SRE books | Burn-rate calculator, alarms, and the local rollback trigger (DR-13) |
| Back-of-envelope estimation | Stated assumptions | AIS feed sizing method (DR-14) |
| Request Pipeline | Joshi, *Patterns of Distributed Systems* | Overlapping API requests, design only (DR-15) |
| PACELC and consistency | Kleppmann, chapters 5 and 9 | Postgres writes and read-model reads (DR-16) |

---

## Sources

- Martin Kleppmann, *Designing Data-Intensive Applications*, chapters 4, 5, 6, 9, 11, and 12.
- Unmesh Joshi, *Patterns of Distributed Systems*, and the pattern articles at martinfowler.com/articles/patterns-of-distributed-systems/.
- Michael Nygard, *Release It!*, for Circuit Breaker, Bulkhead, Timeout, Steady State, and Load Shedding.
- Sam Newman, *Building Microservices*, 2nd edition, for sagas, resilience, and deployment patterns.
- Chris Richardson, microservices.io, for Saga, Transactional Outbox, and CQRS.
- Martin Fowler's bliki articles cited above.
- The Google SRE books (sre.google/books), for SLOs, error budgets, and burn-rate alerting.
- Jay Kreps, "Questioning the Lambda Architecture", O'Reilly Radar, 2014.

This document explains the design and the patterns it applies. Several Harbormaster components are design only, and several war stories are anticipated. Each record states what is built, so a reader can tell the design from the running code.
