# Platform war stories

This file records debugging episodes from building Harbormaster. Entries P1 to P8 are anticipated failure modes that no run has observed, and each one is labeled. The later entries rest on a drill, a live run, a review finding, or test evidence, and each status line says which one. Each entry keeps the wrong first hypothesis, because readers usually learn the most from that part.

Public-copy note: the original log cited private commit hashes, drill transcripts, phase plans, and runbooks as grounding artifacts. Those artifacts are not part of this public repository. This copy keeps the code paths that remain and says where a transcript was used. The AWS demonstration environment is no longer running, and this repository deploys nothing by default.

## Format

Every entry follows the same five parts:

1. **Symptom:** This part records what was observed, with the concrete signal, such as an error, a metric, a bill line, or a stuck pipeline.
2. **Wrong first hypothesis:** This part records what I first believed and acted on, before the evidence corrected me.
3. **Root cause:** This part explains what was actually happening.
4. **Fix:** This part names the specific change that resolved it.
5. **Lesson:** This part states the general takeaway.

## Tagging

Every entry is a personal-build episode, because Harbormaster is personal work that contains no employer code or data ([HONESTY.md](HONESTY.md)). The tags describe the nature of each bug:

- **CONCURRENCY:** This tag marks race conditions, ordering, backpressure, and state coordination.
- **CORRECTNESS:** This tag marks wrong results, data loss, and schema or semantics bugs.
- **TOOLING:** This tag marks provider, build, infrastructure-as-code, dependency, or environment issues.
- Some entries also carry a narrower tag, and a few carry only a narrower tag. These tags are MLOPS, RELIABILITY, ML-RELIABILITY, RL-SAFETY, SECURITY, NETWORKING, CI, OBSERVABILITY, and SERVING/SLO.

## Grounding rule

An entry counts as grounded only when a real artifact backs it, such as a commit, a log excerpt, a drill result, or a `file:line` reference from the build. Anticipated entries are predictions of where the build could fail, written in advance so that a later run could confirm or correct them. No anticipated entry is presented as something that already happened. The status line of each grounded entry names the run in plain words. The workflow numbers in the README refer to the five workflows and not to these runs.

---

## P1: Kinesis shard hot-partitioning on vessel MMSI

**Tags:** CONCURRENCY

**Status:** ANTICIPATED. This entry is a prediction, and no live run, commit, or log observed it.

- **Symptom:** One Kinesis shard runs hot and throttles with `ProvisionedThroughputExceededException`, while its sibling shards stay nearly idle. End-to-end feature latency spikes for a subset of vessels.
- **Wrong first hypothesis:** The stream is under-provisioned overall, so it needs more shards.
- **Root cause:** The partition key is a coarse region code, so a few dense shipping lanes map all their traffic onto one shard. Total throughput is fine, but the key distribution is skewed.
- **Fix:** Repartition on a higher-cardinality key, such as a hash of the MMSI, so per-vessel traffic spreads evenly. Add a hot-key metric so that skew becomes visible before it throttles.
- **Lesson:** Low partition-key cardinality causes the hot shard, and adding shards only hides it. Graph the throughput of each shard as well as the total.

## P2: Flink event-time windows never fire under late AIS

**Tags:** CORRECTNESS

**Status:** ANTICIPATED. This entry is a prediction, and no live run, commit, or log observed it. The current job has no event-time windows ([ADR 0001](adr/0001-streaming-per-event-realization.md)), so this entry applies to the target design.

- **Symptom:** Per-vessel feature windows in Flink stop emitting. The online feature store goes stale even though raw events keep arriving.
- **Wrong first hypothesis:** The Flink job is wedged or the sink is down, so a restart should fix it.
- **Root cause:** The watermark stalls because a handful of vessels emit timestamps far in the future or far in the past. Those timestamps drag the watermark and keep windows from closing. The job is healthy, and the watermark strategy is the problem.
- **Fix:** Add bounded out-of-orderness with an idleness timeout, and clamp obviously bogus timestamps at ingest. Route the clamped records to a side output for inspection.
- **Lesson:** In event-time streaming, a stuck pipeline usually has a watermark problem and not a liveness problem. Guard the watermark against adversarial timestamps.

## P3: Debezium snapshot locks the RDS source during initial CDC

**Tags:** CONCURRENCY

**Status:** ANTICIPATED. This entry is a prediction, and no live run, commit, or log observed it.

- **Symptom:** When CDC is first enabled, queries against the operational RDS Postgres slow sharply. The connector also takes a long time to reach streaming mode.
- **Wrong first hypothesis:** RDS is undersized, so the instance needs to scale up.
- **Root cause:** The default Debezium snapshot reads the whole table set before streaming starts, and that read contends with live traffic. The snapshot strategy is the bottleneck, and the instance size is not.
- **Fix:** Switch to an incremental snapshot, and confirm that `wal_level=logical` and the replica identity are set correctly. Schedule the initial snapshot for a low-traffic window.
- **Lesson:** CDC has a cold-start cost. Plan the snapshot like a migration, and do not treat it as a config toggle.

## P4: SageMaker async endpoint silently drops bursts

**Tags:** CORRECTNESS

**Status:** ANTICIPATED. This entry is a prediction, and no live run, commit, or log observed it.

- **Symptom:** During traffic bursts, some Pi-DPM inference requests produce no result, and no error reaches the caller.
- **Wrong first hypothesis:** The model container crashes on certain inputs.
- **Root cause:** The async endpoint's internal queue overflows past its limit and sheds requests. Without the failure-path SNS notification, the drops stay invisible.
- **Fix:** Wire the async endpoint's success and failure SNS topics, and set autoscaling on the backlog-per-instance metric. Make the caller treat "no result within the SLA" as an explicit retry.
- **Lesson:** An async endpoint is a queue, and a queue has a drop policy. Without the failure notification, dropped requests go unseen.

## P5: DynamoDB online store throttles on cold feature reads

**Tags:** CONCURRENCY

**Status:** ANTICIPATED. This entry is a prediction, and no live run, commit, or log observed it. The current scorer does not read the feature store, so this entry applies to the target design.

- **Symptom:** The scoring front door sees higher p99 latency and `ProvisionedThroughputExceeded` errors on first lookups for vessels not seen recently.
- **Wrong first hypothesis:** The table needs a fixed, higher provisioned capacity.
- **Root cause:** Lookups for vessels not seen recently arrive in clusters, so the reads are bursty and exceed the table's steady provisioned capacity.
- **Fix:** Move the online store to on-demand capacity, or add autoscaling with a burst buffer. Add a short-TTL cache in the front door for hot vessels.
- **Lesson:** Match the capacity mode to the access pattern. Spiky, unpredictable reads suit on-demand capacity better than a guessed provisioned number.

## P6: Iceberg small-file explosion from streaming Firehose writes

**Tags:** CORRECTNESS

**Status:** ANTICIPATED. This entry is a prediction, and no live run, commit, or log observed it. In this snapshot Firehose lands raw JSON, so this entry applies to the target design.

- **Symptom:** Lakehouse query times degrade steadily over days. Reproducible training-data pulls to the GPU training cluster also get slower.
- **Wrong first hypothesis:** The queries need better partition predicates.
- **Root cause:** Firehose lands many tiny objects. Without compaction, Iceberg accumulates thousands of small files plus stale snapshots, so every scan opens a huge number of files.
- **Fix:** Schedule Iceberg compaction (rewrite data files) and snapshot expiration, and tune Firehose buffering toward larger objects.
- **Lesson:** A streaming sink into a table format is a maintenance commitment. Compaction and snapshot expiry are part of the design.

## P7: Budget action attaches deny but does not stop in-flight spend

**Tags:** CORRECTNESS

**Status:** ANTICIPATED. This entry is a prediction, and no live run, commit, or log observed it.

- **Symptom:** The hard-cap budget action fires and attaches the deny policy to the platform role, but spend continues for a while afterward.
- **Wrong first hypothesis:** The budget action did not actually fire, so the guardrail is broken.
- **Root cause:** The deny policy blocks only new actions by the platform role. Resources that are already running, such as an endpoint or an active stream, keep billing. The budget also evaluates on a delay. The guardrail worked as designed, and the mental model was wrong.
- **Fix:** Pair the deny action with the teardown Lambda (`infra/lambda/teardown/`), so a breach also stops or deletes the expensive running resources. Document the budget evaluation delay, so that the tiered soft-budget alerts serve as the real early warning.
- **Lesson:** A deny policy blocks new spend. It does not stop resources that are already running, so a hard cost cap also needs a scheduled teardown.

## P8: Provider drift forces resource replacement

**Tags:** TOOLING

**Status:** ANTICIPATED. This entry is a prediction, and no live run, commit, or log observed it.

- **Symptom:** An unrelated `terraform plan` proposes to destroy and recreate live resources after a routine `terraform init` upgraded a provider.
- **Wrong first hypothesis:** Someone changed the resource configuration, so the task is to find the offending edit.
- **Root cause:** The AWS provider auto-upgraded to a new minor version with changed defaults or attribute handling. The same configuration now differs from the existing state, and no person changed the resource.
- **Fix:** Pin `aws ~> 5.0` plus `archive` and `random` in `infra/terraform/versions.tf`, and commit a lockfile. Upgrade providers deliberately, each in its own reviewed change.
- **Lesson:** An unpinned provider is an unreviewed dependency on another team's release schedule. Pin it, lock it, and treat a provider bump as a real change. This anticipated case is the reason provider pinning is a hard rule in Harbormaster.

## P9: Replication-slot bloat: a stalled CDC consumer pins WAL until the source disk fills

**Tags:** CORRECTNESS TOOLING

**Status:** GROUNDED. A local drill against Postgres 16 on 2026-07-03 produced this entry (`scripts/drill_p1_slot_bloat.py`). The transcript is not included here.

- **Symptom:** The CDC consumer was stalled, so nothing drained the `harbormaster_cdc` pgoutput slot. Source-side WAL retention then grew without bound while ordinary writes continued, and the lag grew with every write round in the drill. On the small `db.t4g.micro` RDS instance, this growth would end in a full disk and a crashed database. The database looked healthy the whole time.
- **Wrong first hypothesis:** Disk growth on the Postgres source means table or index bloat, so the fix is to tune autovacuum or add storage. Vacuum does nothing here, because the growth is not in the tables.
- **Root cause:** A logical replication slot obliges Postgres to keep every WAL segment past the slot's `confirmed_flush_lsn` until the consumer confirms it. A stalled consumer never confirms, so the WAL stays pinned. The consumer might be a crash-looping task, a wedged Kafka Connect worker, or a paused demo. `pg_replication_slots` then shows the slot as `active = false` with steadily growing lag, and the drill reproduced that signature.
- **Fix:** The fix has three layers, and all three are in the tree.
  1. Visibility: `cdc/monitor/slot_lag.py` computes the lag of each slot from `pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)`, with `restart_lsn` as the fallback. The drill asserted that `evaluate_lag_alert` fires at its threshold, and it did.
  2. Alerting: `infra/terraform/modules/cdc_monitoring/main.tf` publishes `Harbormaster/CDC ReplicationSlotLagBytes` every minute from a VPC Lambda and alarms to the FinOps SNS topic. It sets `treat_missing_data = "breaching"`, so a dead monitor also pages.
  3. Prevention: `cdc/connector/config.py` sets `heartbeat.interval.ms` and a `heartbeat.action.query` that updates the published `debezium_heartbeat` table in `cdc/schema/ddl.py`. The heartbeat row lets an idle but healthy pipeline advance the slot, so sustained lag points to a stalled consumer.

  Recovery comes from the consumer draining the slot, and the drill's drain returned the lag to zero in one call. The drill uses its own slot, `hm_drill_p1_slot_bloat`, and never touches the pipeline's main slot.
- **Lesson:** A replication slot holds WAL on the source disk until the slowest consumer confirms it. Monitor the slot lag directly, because consumer-side health checks miss zombie states. Alarm when the monitor itself stops reporting, too.

## P10: Duplicate CDC events after a consumer restart: at-least-once delivery and a non-idempotent sink

**Tags:** CONCURRENCY CORRECTNESS

**Status:** GROUNDED. A local drill through the real applier on 2026-07-03 produced this entry. The transcript is not included here. The tests in `cdc/tests/test_applier.py` and `cdc/tests/test_applier_multipartition.py` cover the redelivery and rebalance schedules.

- **Symptom:** The drill simulated a consumer crash between the sink acknowledgement and the offset commit. In the unguarded configuration, redelivery re-applied already-applied events, and the audit trail showed double writes. The drill then simulated a zombie redelivery after a group rebalance. The unguarded sink let a stale event win, so the watchlist row's severity regressed and the analyst's newer edit vanished. The online item then carried an older LSN than one that had already applied.
- **Wrong first hypothesis:** Whole-row upserts are naturally idempotent, so at-least-once redelivery is harmless, and an in-order replay converges to the same state. The drill's first schedule shows exactly this, because a full in-order replay through the unguarded sink happens to converge. That is why this bug passes testing. The convergence comes from the schedule and not from the sink.
- **Root cause:** At-least-once transport leaves redelivery windows, such as a crash before the offset commit. After a rebalance, a zombie consumer can also re-apply events that its replacement already processed, out of order across the group generation. A last-write-wins upsert has no defense, so whichever delivery arrives last becomes the truth, even a stale one.
- **Fix:** The fix combines the LSN-guarded idempotent sink with the commit protocol. Both are in the tree, and the drill exercised both through the real code path. Every online item carries `last_applied_lsn`, and every write is a whole-item conditional put, `attribute_not_exists(last_applied_lsn) OR last_applied_lsn < :lsn` (`cdc/sinks/dynamo.py`). Deletes write a guarded soft-delete marker, so replays cannot resurrect rows. `cdc/consumer/applier.py` commits Kafka offsets only after every sink acknowledges the batch. Under the guard, both drill schedules converged byte for byte to the exactly-once baseline. The redeliveries stayed visible in the audit trail as `applied=false` rows, which keeps transport truth and state truth separate.
- **Lesson:** Effectively-once state comes from at-least-once transport plus an idempotent sink. The idempotency key must encode order with a monotonic LSN as well as identity with the primary key. Test the zombie redelivery schedule as well as the clean replay.

## P11: Training-serving skew: the holdout gate cannot see a bug that the offline export produced

**Tags:** CORRECTNESS MLOPS

**Status:** GROUNDED. A local drill through the real gate and shadow-diff code on 2026-07-04 produced this entry (`scripts/drill_l1_training_serving_skew.py`). The transcript is not included here.

- **Symptom:** A candidate standardizes a feature as `(x - mean) / std` in its offline training-set export. On the online serving path it receives the raw, unstandardized value. The candidate passes the holdout gate cleanly. The mismatch appears only in the paired-score comparison on the online-encoded copy of the same synthetic drill batch, where the mean absolute score divergence fails its threshold.
- **Wrong first hypothesis:** A clean holdout AUC and calibration ratio mean the candidate is safe to promote. Both metrics come entirely from the offline-encoded holdout set. A bug at the boundary between the offline and online encodings is therefore invisible to them, however clean the numbers look.
- **Root cause:** The holdout gate asks whether the model is good at the task it was evaluated on. It does not ask whether the serving path feeds the model what it expects. Those questions differ whenever training and serving compute a feature separately, and here a standardization step existed in one path and not the other.
- **Fix:** `score_diff` in `mlops/shadow_diff.py` compares the paired scores on the online-encoded copy before any canary weight is set, and it exists to catch this class of bug. The promotion state machine in `mlops/promote.py` never lets a candidate reach canary without a clean shadow result. This comparison is local policy and test evidence, and it is not a managed shadow deployment.
- **Lesson:** An offline metric is only as trustworthy as the assumption that offline and online code compute the same thing. Training-serving skew breaks that assumption, so test the real serving path as well as the holdout set.

## P12: A candidate that passes every offline check regresses only at a later canary weight

**Tags:** MLOPS RELIABILITY

**Status:** GROUNDED. A local drill through the real promotion state machine on 2026-07-04 produced this entry (`scripts/drill_l2_canary_rollback.py`). The transcript is not included here.

- **Symptom:** A candidate had a clean holdout gate and a clean shadow result, because the shadow sample never included the input distribution that triggers the regression. It advanced cleanly through canary weight 5, and then the SLO error budget started burning at weight 25. `run_promotion` in `mlops/promote.py` set weights `[5, 25]` and stopped. It called `revert_to_champion()`, and the transition sequence ended at `canary_25: revert` without reaching 50 or 100.
- **Wrong first hypothesis:** If holdout and shadow both pass, the candidate is safe, and canary is a formality before full rollout. The drill shows why this is false. A shadow sample covers only a fraction of the input distribution, so a rare input pattern can pass through a whole shadow run by chance. This is the P10 lesson again, applied to sampling instead of scheduling.
- **Root cause:** Each pre-production check is a finite sample of the input distribution. In the design, higher canary weights expose more of that distribution, so the canary ramp is the layer that meets the remaining inputs before a rollout becomes irreversible.
- **Fix:** The fix is the promotion saga's compensating action (DR-3 in [SYSTEM_DESIGN_DECISIONS.md](SYSTEM_DESIGN_DECISIONS.md)). A burn-rate breach at any canary weight in the local promotion state machine triggers a full, immediate, one-step revert to the prior champion. A parametrized test in `mlops/tests/test_promote.py` checks the revert at each of the four canary weights, beyond the one weight this drill exercises.
- **Lesson:** Offline and shadow checks cover the known failure modes. A graduated canary ramp is designed to catch the unknown ones, so its rollback must be automatic, immediate, and exercised in CI.

## P13: PyFlink's Python UDF worker is not the driver's Python environment

**Tags:** TOOLING CORRECTNESS

**Status:** GROUNDED. The episode happened on 2026-07-04 in the first streaming window on AWS, which was the first real Managed Flink run of the feature job. The fix is in `streaming/flink/job.py`.

- **Symptom:** `FeatureProcess`, a `KeyedProcessFunction`, failed with `ModuleNotFoundError: No module named 'flink'`. The top-level `from flink.transforms import ...` in `job.py` had run fine at driver startup. After that fix, the same class failed with `ModuleNotFoundError: No module named 'boto3'` from its lazily imported DynamoDB client, although `boto3` was importable everywhere else in the container.
- **Wrong first hypothesis:** If the driver process can import a module, the job can use it anywhere, including inside the per-record callback of a stateful operator. Two attempts to ship the local `flink` and `features` packages as explicit dependencies hit real bugs in Managed Flink's staging of Python dependencies. The first attempt used `env.add_python_file()`, and the second used the `pyFiles` runtime property with two comma-separated paths. Both reproduced a `FileAlreadyExistsException` deterministically, with different cache hashes each time, before the real root cause appeared.
- **Root cause:** A stateful `KeyedProcessFunction` runs inside a separate Python UDF worker subprocess, which is Apache Beam's process-mode portability harness. That worker does not inherit the driver's `sys.path` or installed packages. cloudpickle serializes a function or class by value only when `__main__` defines it. Anything imported from a real package, such as `flink.transforms` or `features.features`, is pickled by reference, so the worker must import that exact module from its own `sys.path`. Third-party packages have the same problem, because `boto3` sits in the driver's site-packages and not in the worker's.
- **Fix:** The logic of the two local packages was inlined into `job.py` as a documented, deliberate duplicate of the tested source files. The referenced code then lives in `__main__` and serializes by value, with no dependency-staging step. For the third-party `boto3`, the job uses `env.set_python_requirements()` with a one-line `requirements.txt`. That single-file mechanism is more reliable than `pyFiles`, and the AWS `PythonDependencies` example is built around it.
- **Lesson:** A module that imports in the driver may still fail to import in a stateful operator's worker. On Managed Flink, avoid `pyFiles` and `add_python_file` for anything beyond a single well-tested dependency path. Inlining tightly coupled local helpers into the entry-point module avoids the distributed-cache staging subsystem entirely.

## P14: A replay fixture's historical timestamps make an online store's TTL fire on arrival

**Tags:** CORRECTNESS

**Status:** GROUNDED. The episode happened on 2026-07-04 in the first streaming window on AWS, and it shares the fix set of P13.

- **Symptom:** The Flink job ran with zero errors, and the DynamoDB writes succeeded. A table scan still showed the item count fluctuating across checks a few seconds apart and then falling to zero. Neither a known target record nor a fresh, manually injected test record ever persisted.
- **Wrong first hypothesis:** The Kinesis consumer must be stalled or missing records. Much of the debugging time went to ruling out stream-position timing, shard assignment, and a genuine consumer stall through `numRecordsIn`, CloudWatch metrics, and direct `put-record` tests.
- **Root cause:** `feature_item()` computes the DynamoDB `ttl` attribute from the AIS fix's own event time, which is correct for live data. The replay fixture's timestamps come from June 2024, and the table enables TTL on that same `ttl` attribute. Every write was therefore more than a year past its expiry the moment it landed. The DynamoDB TTL sweeper deleted items within seconds, which made a stalled consumer look like the more plausible explanation.
- **Fix:** The DynamoDB write call site now overrides `ttl` with a wall-clock value (`time.time() + 7*86400`). The shared, unit-tested `feature_item()` function stays unchanged, because its formula is correct for real event times.
- **Lesson:** When a demo or backfill replays historical data through a live pipeline, check every system that derives a deadline from the payload's timestamp. TTLs, cache expiry, and retry backoff all assume that the timestamp is roughly now. A count that fluctuates and then empties is a strong signal to check the TTL configuration before you chase consumer theories.

## P15: A precomputed-feature payload silently drifted from the real serving schema

**Tags:** CORRECTNESS

**Status:** GROUNDED. The episode happened on 2026-07-04 in the first streaming window on AWS, and it shares the fix set of P13 and P14.

- **Symptom:** The Flink pipeline was fixed, and the DynamoDB writes landed correctly, but the HITL review queue stayed empty. The serving API's access logs showed every `POST /v1/score-ais` call from Flink returning `422 Unprocessable Entity`. The failures were silent because the scorer call was written as best-effort (`except URLError: pass`).
- **Wrong first hypothesis:** The first reading was that the pipeline worked and that nothing anomalous enough to flag had arrived. That reading held because `urllib.request.urlopen` never raised past the catch block in a visible way, and it stayed plausible until someone read the serving logs.
- **Root cause:** `score_request()` built a flat payload that embedded the vessel's precomputed `WindowFeatures` under a `"features"` key. The `AisScoreIn` Pydantic schema in `serving/app/models.py` has never had a `features` field. It expects `{mmsi, fix, history}` and recomputes the anomaly features on the server from the raw fix and history. The two contracts had drifted apart, and nothing kept them in sync, because they live in different subsystems with separate test suites.
- **Fix:** `score_request()` now sends `{mmsi, fix: {...}, history: [...]}`. It passes Flink's keyed previous-fix state as history, so the scorer sees the same points that Flink used for its cheap gate. The function's unit test now asserts the real shape.
- **Lesson:** A best-effort try/except around a cross-service call can hide schema drift for a long time. The failure never surfaces as an exception where a developer is likely to look, and the dependent code simply receives nothing. Read the receiving service's logs before you conclude that a downstream system has nothing to flag.

## P16: A deterministic planner silently skips a detector below its history threshold

**Tags:** CORRECTNESS

**Status:** GROUNDED. The episode happened on 2026-07-04 in the first streaming window on AWS. The fix keeps a rolling history window in the Flink job.

- **Symptom:** After the schema fix in P15, 200 OK responses flowed, and the HITL queue received an `off_corridor` anomaly for one MMSI. The fixture's documented gap anomaly never appeared. That anomaly is a 180-minute AIS silence gap for the vessel that has MMSI 200000001 in the current fixture. It always scored `n_reasons=0` even though it reached the scorer. During this window the fixture used an earlier synthetic MMSI. The fixture later moved every MMSI to the MID (Maritime Identification Digits) prefix 200, which the ITU does not allocate to any country.
- **Wrong first hypothesis:** The gap-detection agent's severity threshold must not consider this gap severe enough to flag. The story was plausible, because P_phys for this gap is 1.0. The vessel reappears at a position that is easy to reach at normal speed, so nothing about the gap looks kinematically impossible.
- **Root cause:** `HeuristicPlanner` in `serving/app/agents/heuristic_planner.py` routes by history length. It adds the abnormal-gap detector (`GapDetectorAgent`) to the plan only when `n_history >= 3`. Flink's keyed state tracked only the most recent fix, so every `score-ais` call carried exactly one history entry. The gap detector was never in the plan, so its severity and threshold never mattered. A direct `curl` of the exact payload for the gap-crossing record, sent with five history entries, returned `abnormal_gap` and `hitl_required: true`.
- **Fix:** `FeatureProcess` now keeps a rolling window of the last five fixes in keyed state. The state is still a plain JSON-encoded `ValueState`, and `score_request()` sends the full retained history.
- **Lesson:** A routing layer that conditions on the shape of its input can leave a whole code path dead, and no single call fails or looks wrong. "The request scored successfully" and "the detector evaluated the request" are different claims. Verify the second claim directly with a payload that should trigger the detector.

## P17: The job's own IAM role never had permission to read its own code

**Tags:** TOOLING

**Status:** GROUNDED. The episode happened on 2026-07-04 in the first EMR run on AWS. This was one of four bugs fixed from that run.

- **Symptom:** The first real `aws emr-serverless start-job-run` failed at driver startup with `java.io.FileNotFoundException: File s3://.../code/lake_backfill_job.py does not exist`. A direct `aws s3 ls` on that exact key showed that the object existed.
- **Wrong first hypothesis:** The upload failed silently, or S3 eventual consistency delayed the object between the `aws s3 cp` and the job start.
- **Root cause:** The job-execution IAM policy in `modules/emr_backfill` granted `s3:GetObject` and `s3:ListBucket` on exactly two prefixes. One was the raw-extract input path, and the other was the output path `<lake_bucket>/iceberg/*`. No statement granted read access to `<lake_bucket>/code/*`, where `scripts/package_lake_for_emr.sh --upload` puts the entry-point script, the `--py-files` zip, and the venv archive. The EMR Serverless driver uses the job's execution role for every S3 call, including the fetch of its own entry point. IAM denied the read, and Spark's JVM error handling reported it as `FileNotFoundException` and not as `AccessDenied`, which made the upload theory plausible.
- **Fix:** A new `ReadJobCode` IAM statement grants `s3:GetObject` and `s3:ListBucket` on `<lake_bucket>/code/*`.
- **Lesson:** A least-privilege IAM policy covers the resources its author expected the job to touch. The job's own code and dependency artifacts are easy to forget, because they seem like packaging details. Suspect a denied read before you suspect the upload. That rule applies when a JVM runtime backed by an AWS SDK reports that an existing object "does not exist".

## P18: A Python 3.10 idiom breaks on the EMR runtime's Python 3.9

**Tags:** TOOLING

**Status:** GROUNDED. The episode happened on 2026-07-04 in the first EMR run on AWS, and it shares the fix set of P17.

- **Symptom:** After the IAM, schema, and region fixes from earlier attempts in the same run, the job failed with `TypeError: zip() takes no keyword arguments`. The error came from the corridor-graph code, which had passing local unit tests and had never failed locally.
- **Wrong first hypothesis:** A packaging problem caused it, such as a stale `lake_pkg.zip` or the wrong module resolving, because the code had just worked locally.
- **Root cause:** Python 3.10 added the `strict=` keyword to `zip()`. The development machine runs Python 3.12, but the EMR Serverless `emr-7.2.0` Spark image ships Python 3.9.21, which a direct `docker run ... python3 --version` confirmed. The project's ruff configuration (`B905`) requires an explicit `strict=` on every `zip()` call. That rule is good practice on Python 3.10 and newer, but it breaks code that must run on this older, fixed runtime.
- **Fix:** `strict=` was removed from the three call sites, each with `# noqa: B905`, so the lint rule stays in force for every other file. At each site the paired sequences come from the same source array or the same grouping, so they always have equal length and `strict=` only documented that fact.
- **Lesson:** A job that runs on a local Python can still fail on a managed service with its own fixed image, such as EMR Serverless, Lambda, or Glue. The target's Python version limits which language features the job can use. Pull the real image and check its version before you assume parity with the development environment.

## P19: A pure function's own docstring predicted the bug in the Spark wiring around it

**Tags:** CORRECTNESS

**Status:** GROUNDED. The episode happened on 2026-07-04 in the first EMR run on AWS, and it shares the fix set of P17.

- **Symptom:** After the IAM fix in P17, the first live EMR job failed with `PySparkTypeError: ... datetime64[ns, UTC] ... Expected a string or bytes dtype`. The error came from the Arrow conversion inside `mapInPandas` on the output of the gate and canonicalize step.
- **Wrong first hypothesis:** The fixture parquet had the wrong dtype for `t`. That was a real, separate issue, and it was fixed first. The ad hoc conversion from fixture to parquet had let pandas infer `t` as `datetime64`, while the raw-read schema declares it as `StringType`. Fixing that alone did not clear the error, which exposed this second bug.
- **Root cause:** In `lake/backfill/job.py`, `mapInPandas(..., schema=raw.schema)` reused the raw input schema (`t: StringType`) as the declared output schema of `_gate_and_canonicalize_partition`. That function returns `canonicalize_positions(pdf)`, whose docstring says that "t is coerced to ... a UTC timestamp." The function stated its contract correctly, and the Spark wiring around it declared the wrong output schema.
- **Fix:** A separate `canonical_schema` (`t: TimestampType`) now matches what `canonicalize_positions` returns, and `mapInPandas` receives it in place of `raw.schema`.
- **Lesson:** When a typed wrapper declares the output schema of a documented function, the schema must describe that function's return shape and not the shape of its input. A docstring that states a transformation is a specification, so check the wiring against it.

## P20: A catalog client needs its own region even when every other AWS client in the process has one

**Tags:** TOOLING

**Status:** GROUNDED. The episode happened on 2026-07-04 in the first EMR run on AWS, and it shares the fix set of P19.

- **Symptom:** After the schema fix, the same job failed with `botocore.exceptions.NoRegionError: You must specify a region`. The error came from the Glue catalog client inside the Iceberg writer. The job ran in `us-east-1`, and `AWS_REGION` was set on the driver and the executors through `spark.emr-serverless.driverEnv` and `executorEnv`.
- **Wrong first hypothesis:** The Spark-level environment variables were not reaching the Python process, so they needed to be set differently or in more places.
- **Root cause:** pyiceberg's `GlueCatalog` reads its boto3 client's region only from catalog properties, `glue.region` with a fallback to the generic `client.region`. It never reads the process environment, instance metadata, or another AWS client in the same process. `catalog_props` (`{"type": "glue", "warehouse": ...}`) set neither property, so the catalog client had no region source at all.
- **Fix:** `catalog_props` now sets `"glue.region"` and `"client.region"` from the same `AWS_REGION` variable. The `client.region` key comes from pyiceberg's property constant in `pyiceberg/io/__init__.py`, which was read to confirm it.
- **Lesson:** A library's catalog or client wrapper can have a narrower configuration surface than the usual AWS client rules. When a client reports a missing setting that the rest of the process already has, check which sources that specific client reads.

## P21: A modern Docker build produces a manifest format that SageMaker's API rejects

**Tags:** TOOLING

**Status:** GROUNDED. The episode happened on 2026-07-04 in the first SageMaker window on AWS, which deployed the demo stand-in. This image-format fix came before the ENTRYPOINT fix in P22.

- **Symptom:** `terraform apply` failed to create `aws_sagemaker_model` with `ValidationException: Unsupported manifest media type application/vnd.oci.image.index.v1+json`. The image had been built and pushed with an ordinary `docker build` and `docker push` on current Docker Desktop.
- **Wrong first hypothesis:** `DOCKER_BUILDKIT=0`, the old switch that selects the legacy builder, would produce the older manifest format that SageMaker expects. It did remove the multi-platform manifest-list wrapper, which was the `image.index` that SageMaker's error named. The single-platform manifest was still in OCI format (`application/vnd.oci.image.manifest.v1+json`), so a retried apply failed with the same ValidationException, now naming the manifest.
- **Root cause:** Current Docker Desktop has effectively removed the classic builder, so `DOCKER_BUILDKIT=0` no longer changes the builder or its output format. BuildKit and buildx default to OCI media types for both the manifest list and the manifest. SageMaker's `CreateModel` API accepts only the older Docker Distribution v2 schema2 format (`application/vnd.docker.distribution.manifest.v2+json`), and that constraint has nothing to do with the image content.
- **Fix:** The build now uses `docker buildx build --provenance=false --sbom=false --output type=image,name=<repo>:<tag>,push=true,oci-mediatypes=false`, which forces the legacy Docker manifest media type. Before the next Terraform apply, a push followed by `docker manifest inspect` confirmed `mediaType: application/vnd.docker.distribution.manifest.v2+json`.
- **Lesson:** A pushed image proves only that the registry holds it, and a consumer service may still reject its manifest format. When an AWS service rejects an image with a manifest media-type error, inspect the pushed manifest's `mediaType` directly with `docker manifest inspect`. An old fix such as `DOCKER_BUILDKIT=0` can stop working as the tooling changes.

## P22: A container with no ENTRYPOINT cannot answer the platform's invocation convention

**Tags:** TOOLING

**Status:** GROUNDED. The episode happened on 2026-07-04 in the first SageMaker window on AWS. The fix is in `mlops/pidpm_container/demo/`.

- **Symptom:** After the manifest fix in P21, the SageMaker endpoint creation still failed with `CannotStartContainerError. Please ensure the model container for variant champion starts correctly when invoked with 'docker run <image> serve'`.
- **Wrong first hypothesis:** The container image itself was broken, with a bad base image, a missing dependency, or the wrong platform. The same image ruled this out, because it ran fine under a plain `docker run -p 8080:8080 <image>` with no trailing argument.
- **Root cause:** The Dockerfile had a `CMD` that launched gunicorn, and it had no `ENTRYPOINT`. When a container has no `ENTRYPOINT`, any arguments to `docker run <image> <args>` replace `CMD` entirely. SageMaker's real-time inference contract invokes every container as `docker run <image> serve`. With no `ENTRYPOINT` to receive that argument, the container tried to run a nonexistent `serve` binary and exited at once.
- **Fix:** The first fix made a one-line `entrypoint.sh` (`exec gunicorn --bind 0.0.0.0:8080 --workers 1 server:app`) the image's `ENTRYPOINT`. It launched the server whatever argument SageMaker passed, because this container only serves. Before the next apply, a local `docker run -v <real-model-dir>:/opt/ml/model <image> serve` returned a healthy `/ping` 200. That command is the exact SageMaker invocation. The current image uses `entrypoint.py`, which ignores the trailing `serve` argument and starts Gunicorn with a fixed command.
- **Lesson:** A container that runs under a bare `docker run <image>` can still fail under the invocation convention of its deployment target. When a managed service documents how it invokes a container, test that exact command locally, argument included.

## P23: In a shared git working directory, one session's branch switch changes the branch for every session

**Tags:** TOOLING

**Status:** GROUNDED. The episode happened on 2026-07-04 during the first SageMaker window, and it was caught before any damage.

- **Symptom:** A `terraform plan` that had been clean minutes earlier proposed to revert an already fixed and verified Cloud Map DNS record type to its broken configuration (`SRV` back to `A`). That broken record had caused an earlier serving-API outage. The plan also wanted to destroy and recreate the live Managed Flink application.
- **Wrong first hypothesis:** The fix had been lost or never committed. This idea was considered briefly and rejected before any action.
- **Root cause:** Two sessions were working against the same on-disk git clone, one at a separate terminal and one running `git`, `terraform`, and `docker` commands. They did not use separate worktrees. A `git checkout` of an older branch in one session changed HEAD for both, with no notice to the other. The older branch (`phase4-flywheel`) had branched off `phase3-lake` before the Cloud Map fix and before that day's live debugging work. Every file on disk therefore reverted to that earlier snapshot for the other session.
- **Fix:** The first step was `git branch --show-current`, as soon as the unexpected diff appeared. An explicit `git checkout` then returned to the working branch, and `git status` and `git branch --show-current` confirmed it before any further Terraform command. No AWS resource was destroyed, because the plan step is a review and surfaced the mismatch before an apply could act on it.
- **Lesson:** In a shared working directory without a worktree per session, the current branch is mutable shared state. Any session can change it for every other session without a signal. When a plan or diff contradicts recent verified work, run `git branch --show-current` first, because it is the cheapest way to confirm which branch is on disk.

## P24: A promotion loop that moves faster than the infrastructure it drives

**Tags:** TOOLING RELIABILITY

**Status:** GROUNDED. The episode happened on 2026-07-04 in a real promotion-pipeline run against the live SageMaker demo endpoint.

- **Symptom:** The real `mlops.promote.run_promotion` loop drove a real `boto3` `update_endpoint_weights_and_capacities` call through `set_canary_weight`. It succeeded at canary weight 5 and then failed at weight 25 with `ClientError: ValidationException: Cannot update in-progress endpoint`.
- **Wrong first hypothesis:** The IAM role or the API call itself was malformed. Both were confirmed correct, because the same call had just succeeded once.
- **Root cause:** `UpdateEndpointWeightsAndCapacities` is asynchronous. The endpoint moves to `Updating` and stays there for tens of seconds before it returns to `InService`. `run_promotion` calls `set_canary_weight` for each weight in immediate succession. That behavior is correct for a pure, synchronous, in-memory state machine, whose unit tests use fakes that return instantly. It breaks once `set_canary_weight` calls a real asynchronous cloud API, because the second call arrives while the first update is still applying. SageMaker rejects concurrent updates outright and does not queue them.
- **Fix:** The injected `set_canary_weight` and `revert_to_champion` callables now poll `describe_endpoint` until `EndpointStatus == "InService"` after each weight update, before they return control to the loop. `run_promotion` itself stays a pure, fast, synchronous state machine that matches its tests.
- **Lesson:** The gap between code tested against fakes and code wired to a real service shows up at the dependency-injection seam of a pure function. The pure function's own tests cannot reveal it, because they supply instant fakes by design. When a real asynchronous client sits behind an interface built for synchronous callables, put the waiting in the adapter and keep the core logic as it is.

## P25: An audit finding on paper and a live-verified fix are different claims

**Tags:** CORRECTNESS TOOLING

**Status:** GROUNDED. The episode happened on 2026-07-04 on the SageMaker demo endpoint. The fix was applied, and its effect was confirmed live.

- **Symptom:** A freshly deployed SageMaker async endpoint (`initial_instance_count = 1`, `ml.g4dn.xlarge`) reported healthy and returned valid responses to real invocations. No error, warning, or log line pointed at its autoscaling configuration.
- **Wrong first hypothesis:** The original code assumed that scale-to-zero would happen on its own once `terraform apply` succeeded and both the target-tracking policy and the step-scaling policy existed without errors.
- **Root cause:** The `customized_metric_specification` block of the target-tracking scale-in policy named only `metric_name`, `namespace`, and `statistic`, with no `dimensions` block. SageMaker publishes `ApproximateBacklogSizePerInstance` under the `EndpointName` dimension. Application Auto Scaling requires a policy to specify the same dimensions that its metric was published with, or the policy queries a metric series that does not exist. The alarms behind the policy therefore stayed in `INSUFFICIENT_DATA` and could never fire. Scale-in to zero could never happen on this configuration, however long the endpoint sat idle.
- **Fix:** A `dimensions { name = "EndpointName", value = aws_sagemaker_endpoint.pidpm.name }` block now sits inside `customized_metric_specification`. An earlier independent audit had found this gap by comparing the code with AWS's canonical example notebook and API reference, before the failure appeared live. The fix went out through a clean, single-resource `terraform apply` before idle cost built up. After the fix, the two `TargetTracking-...` CloudWatch alarms showed fresh data points and left `INSUFFICIENT_DATA`.
- **Lesson:** "The resource exists and nothing errored" is a weaker claim than "the mechanism this resource provides works." For an alarm stuck in `INSUFFICIENT_DATA`, no exception, log line, or red status reveals the gap. An independent audit against the provider's own examples caught this bug early, so treat an audit's fix-before-demo findings as blocking.

## P26: The same class of bug survived the targeted audit fix in a second alarm

**Tags:** CORRECTNESS

**Status:** GROUNDED. The episode happened on 2026-07-04 during the live scale-out-from-zero check, in the same window as the P25 fix.

- **Symptom:** The P25 scale-in fix worked, and the endpoint reached zero instances. A real invocation was then sent to wake it from zero. `ApproximateBacklogSize` showed a persistent backlog for a sustained period, but the endpoint's `DesiredInstanceCount` never moved off zero. The `HasBacklogWithoutCapacity` CloudWatch alarm exists to detect this condition and trigger the scale-out-from-zero step policy, and it reported `StateReason: no datapoints were received` the whole time.
- **Wrong first hypothesis:** This metric simply had a longer publish latency than `ApproximateBacklogSize`, so more patience would resolve it. This idea was entertained briefly before the check.
- **Root cause:** The Terraform for the `has_backlog_without_capacity` alarm declared `dimensions = { EndpointName = ..., VariantName = ... }`. AWS's canonical scale-from-zero example (`docs.aws.amazon.com/sagemaker/latest/dg/async-inference-autoscale.html`) specifies only `EndpointName` for this metric. A direct CloudWatch query confirmed it. `get-metric-statistics` for `HasBacklogWithoutCapacity` with `EndpointName` alone returned real datapoints at the same moments when the two-dimension query returned nothing. SageMaker does not publish this metric under a `VariantName` dimension, so the extra dimension queried a series that never existed. The bug had the same shape as P25, with an added dimension in place of a missing one.
- **Fix:** The `VariantName` dimension was removed from the alarm, which now matches AWS's canonical pattern. In the same session, the live check showed the alarm move to `ALARM` right after the apply. `DesiredInstanceCount` then moved to 1, and the endpoint reached `InService` shortly afterward. The full round trip from zero instances back to serving was observed end to end.
- **Lesson:** Fixing one instance of a bug class does not make every similar configuration safe. Each metric that AWS publishes has its own set of dimensions, and copying the pattern of a recently fixed alarm adds dimensions by analogy. When a metric never receives data, treat a wrong dimension set as a leading hypothesis, and query CloudWatch directly with different dimension combinations.

## P27: Cross-checking two drift proxies stops a false concept-drift retrain

**Tags:** ML-RELIABILITY

**Status:** GROUNDED. A local drill on 2026-07-04 produced this entry. The drill ran through the real drift, calibration, concept-proxy, and decision-table code (`scripts/drill_l3_drift_classification.py`). The transcript is not included here.

- **Symptom:** Proxy 1 (`flag_uncertain_trace` in `mlops/concept_proxy.py`) flags traces near the HITL routing threshold with high Pi-DPM epistemic variance. In the drill it fires on every synthetic trace in the batch, which a naive design would read as concept drift. The HITL disagreement rate for the same batch, proxy 2 (`disagreement_rate`), stays at zero, because every labeled fixture row in the drill agreed with the model. `classify_drift` in `mlops/drift_decision.py` correctly does not return `concept_drift` for this combination.
- **Wrong first hypothesis:** A rising volume of near-threshold, high-uncertainty traces is concept drift, so it should trigger the preference pipeline. That view conflates two different situations. The model may be meeting more ambiguous cases, or it may now be wrong about cases it used to get right. Proxy 1 cannot tell these apart, because it never touches a signal close to ground truth.
- **Root cause:** Production anomaly detection has no ground-truth label stream, so no direct concept-drift detector exists. Proxy 1 measures the shape of the population, and it does not measure correctness. Only proxy 2, the rate at which human reviewers override the model, sits close to ground truth, although it lags. Treating proxy 1 alone as sufficient would trigger retraining when the model is seeing harder cases that it still gets right. The decision table exists to prevent that false alarm.
- **Fix:** `classify_drift` applies a precedence rule. A rising proxy 2 means concept drift regardless of proxy 1. A rising proxy 1 with a flat proxy 2 routes only to `log_only`. The drill's fourth scenario builds exactly this false-alarm combination and asserts that the decision is not `concept_drift`.
- **Lesson:** A single proxy signal does not replace one that sits close to ground truth when the two answer different questions. Cross-check a fast, noisy proxy against a slow, trustworthy one before you act on it.

## P28: The reward-hacking probe blocks a gamed checkpoint before it reaches shadow

**Tags:** RL-SAFETY MLOPS

**Status:** GROUNDED. A local drill through the real probe and promotion state machine on 2026-07-04 produced this entry (`scripts/drill_l4_reward_hacking.py`). The transcript is not included here.

- **Symptom:** A synthetic preference-tuned candidate raises its mean total reward over the baseline. It does this by inflating the `shaping`, `data`, and `pref` reward terms. Its `structural` term turns negative on most of the batch, and a negative structural term marks a real kinematic-constraint violation, while the baseline has no such violations. `mlops.reward_hacking_probe.run_reward_hacking_probe` returns `blocked=True`. `mlops.promote.run_promotion` then halts at a new `reward_probe` step right after the holdout gate, and it never reaches shadow or canary (`weights_set` stays empty).
- **Wrong first hypothesis:** A higher mean reward means a better candidate, so reward alone should be a sufficient promotion signal once the holdout gate passes. The structural term in `mlops/preference_builder.py` carries an unbounded, heavily weighted penalty, so the promotion pipeline also checks whether a reward increase came with more physics violations, not only whether the average reward rose.
- **Root cause:** The holdout gate and the reward-hacking probe answer different questions, as in P11. The gate checks predictive quality on a fixed offline set, and the probe checks whether a reward increase came with more physics violations. Neither replaces the other, so the probe is an added step.
- **Fix:** At the time of the drill, the probe blocked a candidate when `candidate_mean_reward > baseline_mean_reward AND candidate_structural_violation_rate > baseline_structural_violation_rate`. The drill's second scenario is an honest candidate with the same reward increase and a flat violation rate. It passes the probe and promotes through the full state machine (`weights_set == [5, 25, 50, 100]`), which shows that the probe does not simply penalize reward increases.
- **Lesson:** A penalty term only works as a safeguard when a downstream check confirms that a reward increase did not come at that term's expense. `structural_violation_in_either_arm` in `mlops/preference_builder.py` supplies that check on every preference triple.
- **Follow-up (later on 2026-07-04):** A later adversarial review found that the rate-only condition cannot see magnitude. A candidate can match the baseline's violation count while making each violation far more severe. The blocking condition in `mlops/reward_hacking_probe.py` is now `mean_up and (rate_up or candidate_mean_structural < baseline_mean_structural)`. The drill's two scenarios use a uniform shift in the structural term, so the hardening does not change their verdicts.

## P29: A checkov baseline regenerated before the code it was supposed to gate

**Tags:** TOOLING CORRECTNESS

**Status:** GROUNDED. The episode happened on 2026-07-06, and its evidence is in `infra/terraform/.checkov.baseline` and the WAF and API Gateway modules. The `iac-ci` checkov job ran in the private repository's CI. This public snapshot's CI does not run checkov.

- **Symptom:** The checkov baseline was refreshed early in the change, and more infrastructure code landed after it, including the gated WAF and the API Gateway access-logging and authorizer work. CI would have run checkov against that early baseline. It would have suppressed the new findings from the later code and reported a clean scan against a stale snapshot.
- **Wrong first hypothesis:** No note of the first hypothesis survives. The recorded sequence assumed that a baseline refreshed before the infrastructure code was final would still expose findings introduced later.
- **Root cause:** A suppression baseline is a point-in-time diff against the tree, and it is not a live policy. A baseline captured before the final security code puts every finding from that code into the accepted set, so the gate cannot see the findings it exists to catch.
- **Fix:** The new findings were fixed in code and not added to the baseline. The WAF gained the Log4j `KnownBadInputs` AWS Managed Rule and access logging, so those findings resolved. Only the accepted brownfield class, 14-day log retention, was re-baselined. The ratchet is the CI check that fails on any finding outside the baseline. It was re-run against the final tree to show that it still fails on anything new.
- **Lesson:** Regenerate a suppression baseline together with the code it gates, in the same change and as the last step. Before you trust a green scan, prove that the ratchet still fails on a fresh finding, because a baseline captured too early accepts everything.

## P30: pyiceberg partition transforms need the pyiceberg_core Rust extension on the write path

**Tags:** TOOLING CORRECTNESS

**Status:** GROUNDED. The episode happened on 2026-07-06 in `lake/iceberg.py`. This was a local write-path failure and fallback, and no cloud lake run was involved.

- **Symptom:** Wiring day and bucket partition transforms into the Iceberg writer raised `NotInstalledError` locally on the write path. The transform objects constructed fine, and the partition-spec API looked complete.
- **Wrong first hypothesis:** The transforms could be declared and the partition-spec objects constructed, so the environment seemed to have everything it needed to apply them during a write.
- **Root cause:** pyiceberg implements its `day()` and `bucket()` partition transforms in the native `pyiceberg_core` Rust extension, which the local environment did not have. The Python API accepts the spec regardless, and the extension is needed only when a write applies the transform to real data. The gap stays invisible until a write runs.
- **Fix:** The writer now falls back to an identity partition when the extension is absent, and it applies the full day and bucket spec when the extension is present. The behavior depends on what the environment can do, and not on whether the API is callable.
- **Lesson:** Verify a library feature on the real write path in the real environment. A constructor that succeeds does not prove that the operation it configures can run, because features backed by native extensions fail at use time.

## P31: Extracting inlined Flink UDFs to a module changes distributed serialization

**Tags:** CONCURRENCY CORRECTNESS

**Status:** GROUNDED. This was a design-time catch on 2026-07-06 in `streaming/flink/window_logic.py` and `streaming/flink/job.py`, and no live worker failed.

- **Symptom:** Pulling the inlined streaming window functions into a testable `window_logic.py` module looked like a pure refactor for test imports. A naive extraction would still have changed how those functions ship to Flink workers.
- **Wrong first hypothesis:** Moving a pure helper from `__main__` into an importable module changes nothing about behavior. This design assumption was caught before any worker failed.
- **Root cause:** cloudpickle serializes a function defined in `__main__`, such as an inlined UDF, by value and ships its bytecode. It ships a function imported from a real package by reference, as a short pointer that the worker must re-import from its own `sys.path`. Moving the functions into a module flips them from by value to by reference, which is the P13 failure mode from the other direction. The worker would then need `window_logic` on its own path, and the extraction does not guarantee that.
- **Fix:** The extracted module is registered with `cloudpickle.register_pickle_by_value`, so its functions still ship by value after the move. The job keeps its original behavior and gains a unit-testable module.
- **Lesson:** A refactor that only moves code can still change how a distributed system serializes it. In a cloudpickle and Flink pipeline, the place where a function is defined affects behavior, so test the serialization boundary as well as the local import.

## P32: A permissions boundary on the deploy identity binds every role that identity creates

**Tags:** TOOLING

**Status:** GROUNDED. This was a design-time catch on 2026-07-06 and 2026-07-07, and no apply failed. The work first defined the boundary contract for the deploy identity and then wired the boundary into every role-creating module. The boundary and the module roles were later applied live in the 2026-07-12 window on AWS. No deployment has run through the boundary-gated `harbormaster-platform` role.

- **Symptom:** A permissions-boundary condition on the deploy identity closes the privilege-escalation path of `iam:*` on `Resource:*`. It changes one principal, but it also creates an obligation at apply time. Every `aws_iam_role` that the deploy identity creates must now set `permissions_boundary`, or `CreateRole` is denied during the apply.
- **Wrong first hypothesis:** Tightening the deploy identity is a local policy edit, and the roles it creates need no matching change. This design assumption was caught before any apply.
- **Root cause:** A boundary that requires the principal to attach a boundary to every role it creates binds two sides. Closing the escalation on the deploy identity is one side. The other side is that no module-defined role can be created until it also carries the boundary. The escalation fix and the role-wide obligation are the same condition, seen from the principal and from the resource.
- **Fix:** `permissions_boundary` now flows through every module `aws_iam_role` that the deploy identity manages. The roles satisfy the boundary condition up front, so the denials do not appear one failed apply at a time.
- **Lesson:** A permissions boundary on one identity constrains the whole set of roles that identity manages. Closing an escalation on the principal creates an apply-time requirement on every downstream role, and both sides must land in the same change.

## P33: Mutation testing as the check against tautological tests for new guards

**Tags:** CORRECTNESS

**Status:** GROUNDED. Test evidence from 2026-07-03 to 2026-07-06 supports this entry. The evidence covers the CDC sink LSN guard with its equal-LSN regression and `serving/app/burn_rate.py` with its boundary tests. The temporary mutations were reverted, and this entry records their historical red runs without re-running them.

- **Symptom:** The new CDC-sink and burn-rate tests all passed on the first run. A new test is least trustworthy at that moment, because a green assertion proves nothing until a deliberate regression turns it red.
- **Wrong first hypothesis:** Green tests on the first run were enough to show that both new guards were covered. This assumption was challenged before shipping, and it did not cause a failure in any run.
- **Root cause:** A passing new test can be a tautology that asserts something the code makes true regardless of the logic under test. Without a mutation, a test that passes and a test that binds the behavior look the same. A guard could therefore ship with tests that never exercise its boundary.
- **Fix:** Targeted mutations checked both guards. Weakening the CDC duplicate guard from a strict `<` to `<=` admitted an equal-LSN redelivery, and the equal-LSN duplicate test failed as intended. Making the burn-rate calculator always return `False` made the burn-rate tests fail. Both regressions turned the suite red, which showed that the tests bind the real behavior, and both mutations were then reverted.
- **Lesson:** A passing new test proves coverage only after a deliberate regression makes it fail. Mutation testing is a cheap check against tautological tests, especially for boundary guards where `<` versus `<=` is the whole point.

## P34: A checkov baseline coupled to a gitignored tfvars file that CI cannot reproduce

**Tags:** CI

**Status:** GROUNDED. Two debugging commits during the first PR CI run on 2026-07-07 support this entry. The `iac-ci` and `serving-ci` jobs ran in the private repository's CI. This public snapshot's CI runs ruff, a secret scan, and the unit tests, and it does not run checkov.

- **Symptom:** The new `iac-ci` checkov gate failed on the PR. It reported findings as new across modules that the branch never touched (`rds`, `ecs_*`, `sagemaker_pidpm`, `apigw`). The identical `checkov -d infra/terraform --baseline infra/terraform/.checkov.baseline` command exited 0 on the laptop. Separately, `serving-ci` failed with `ModuleNotFoundError: No module named 'pyiceberg'` on lake tests that passed locally.
- **Wrong first hypothesis:** The action's newer checkov version explained the whole mismatch, so pinning checkov 3.3.6 would make CI agree with the local run. The first debugging commit applied that fix, and the second commit records why findings still persisted.
- **Root cause:** Two separate mismatches between the local environment and CI caused the failures, and neither was a real regression. First, `bridgecrewio/checkov-action` bundled a newer checkov than the local 3.3.6 that generated the committed baseline. A newer checkov adds and renames checks, so accepted brownfield findings came back as new. Pinning the version helped, but findings persisted because of a deeper cause. The baseline stored resource addresses with module count indices (`module.apigw[0].aws_cloudwatch_log_group.access[0]`), and CI emitted addresses without them (`module.apigw.aws_cloudwatch_log_group.access[0]`). checkov resolves a module `count` from variable values, and the baseline had been generated with a gitignored `envs/base/terraform.tfvars` that sets `enable_phase1 = true`. CI checks out only tracked files and has no tfvars, so it cannot reproduce the indexed keys. The baseline was coupled to the variable resolution of a gitignored file. The pyiceberg failure had a related cause. The `[lake]` extra that CI installs declared only `pyarrow`, which was left over from a time when the lake tests checked only schema shapes.
- **Fix:** A `pip install checkov==3.3.6` step pins checkov to the version that generated the baseline, because `checkov_version` is not an input on that action, as its `action.yml` shows. CI was then reproduced exactly with `git archive HEAD infra/terraform | tar -x`, which yields tracked files only, with no tfvars and no `.terraform` directory. The baseline was regenerated against that tree, so its keys match what CI produces. checkov 3.3.6 then exited 0 on that tree, and the same accepted brownfield set was preserved. The regenerated baseline hid none of the branch's fixes, such as route auth and access logging, and it hid no WAF or critical finding. `pyiceberg[sql-sqlite]>=0.7` joined the `[lake]` extra, and the workflow documents the gitignored-tfvars dependency so that a future regeneration does not reintroduce it.
- **Lesson:** A suppression baseline for checkov, tflint, or semgrep is only as reproducible as the environment that generated it. Generate it the way CI sees the code, from the tracked tree with no gitignored tfvars and no local `.terraform` state, and pin the scanner version. Suspect environment drift before you hunt for a real regression when a scan passes locally but fails in CI on files the change never touched.

## P35: A single tenant's population shift disappears into a global drift average

**Tags:** CORRECTNESS OBSERVABILITY

**Status:** GROUNDED. A local drill on 2026-07-11 produced this entry (`make drill-m-drift-hidden`, `scripts/drill_m_drift_hidden.py`). The transcript is not included here.

- **Symptom:** The input-drift monitor stays green while one tenant's feature distribution has clearly moved. That tenant's model degrades with no alert.
- **Wrong first hypothesis:** The PSI and KS thresholds are too loose, so the alert threshold should be lowered until the shift crosses it.
- **Root cause:** The monitor pooled every tenant's rows into one distribution before it computed PSI. In the drill fixture, the stable tenants diluted one tenant's large shift, and the pooled PSI fell far below the alert threshold. Lowering the global threshold would only replace the missed alert with many false positives on the stable tenants. The pooling is the problem, and the threshold is not.
- **Fix:** `mlops/tenant_drift.py` partitions the windows by `tenant_id` and runs the unchanged `check_input_drift` once per tenant. The shift then appears in that tenant's own result (`drifted_tenants(...) == ["tenant_a"]`), and the average no longer hides it. The drill computes the pooled baseline from the same fixture, so the contrast is measured and not assumed.
- **Lesson:** An aggregate metric over a partitioned population hides regressions in single partitions, and tightening the aggregate threshold cannot recover them without many false alarms. Monitor at the granularity where you make decisions, which here is per tenant. Prove that a global monitor would miss the shift by computing it on the same data.

## P36: Tenant isolation in application code leaks rows when one query omits the tenant predicate

**Tags:** SECURITY CORRECTNESS

**Status:** GROUNDED. A local drill against a real Postgres on 2026-07-11 produced this entry (`make drill-m-tenant-leak`, `scripts/drill_m_tenant_leak.py`). The transcript is not included here.

- **Symptom:** A multi-tenant query path returns another tenant's rows if a developer forgets one `AND tenant_id = :tid` predicate. No test reliably catches the omission, because the happy path looks the same.
- **Wrong first hypothesis:** A careful code review plus a repository helper that always appends the tenant predicate is enough, so isolation is an application-layer discipline.
- **Root cause:** Isolation in the application layer fails open. A hand-written SQL query, a new endpoint, or a direct ORM call can bypass the helper, and any such path returns cross-tenant data. A missing predicate returns everything by default, which is the worst possible default for a security control.
- **Fix:** Isolation moved into Postgres row-level security (`cdc/schema/tenancy.py`). Every tenant table has `ENABLE` and `FORCE ROW LEVEL SECURITY` with the policy `USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)`. The `NULLIF(..., true)` form makes an unset setting match no row, so isolation fails closed. The drill proves this on a real database as a `NOSUPERUSER` owner. Tenant B reads none of tenant A's rows, and a session with no `app.tenant_id` reads no rows from all four tables. The policy's `WITH CHECK` also rejects a write with no tenant (SQLSTATE 42501).
- **Lesson:** A security control whose failure mode is a leak belongs at a layer that code cannot bypass. Row-level security makes cross-tenant isolation a property of the database, and its fail-closed default turns a forgotten `WHERE` into an empty result. Test it as a non-superuser, because superusers bypass row-level security by design.

## P37: A rarely hit tenant pays the full scale-from-zero cold start on its first request

**Tags:** SERVING/SLO

**Status:** GROUNDED BEHAVIOR. It was observed in a bounded EKS and KEDA demonstration window, and this entry makes no SLO breach claim. The run evidence is not included here.

- **Symptom:** The serving deployment began with zero replicas. Under load, KEDA requested one replica, and the first ready pod then served an HTTP 200 after a short cold-start delay. The deployment later returned to zero.
- **Wrong first hypothesis:** Scale-to-zero has no downside, so every idle tenant should scale to zero.
- **Root cause:** Scale-to-zero trades standing cost for tail latency. The live measurement covers the end-to-end delay from the observed KEDA scale request to the first successful response. It does not separate image pull, container start, model load, and health-check time.
- **Fix:** This run did not justify a production change to the minimum replica count. The window measured the tradeoff, returned the front door to ECS, and removed the EKS cluster. A future tenant tier might set a latency threshold below the measured cold-start delay. That tier should keep at least one replica, and scale-to-zero should stay with tiers that can absorb the delay.
- **Lesson:** Scale-to-zero is an SLO decision as well as a cost decision, and the right minimum replica count differs by tier. A measured delay becomes an SLO breach only through an explicit comparison with a threshold.

## P38: A permissions boundary that omits a service silently disables its teardown

**Tags:** TOOLING SECURITY

**Status:** GROUNDED. A pressure-test review on 2026-07-11 found this defect. The fix and its regression test are in `tests/e2e/test_permissions_boundary.py` and `infra/aws/harbormaster-permissions-boundary.json`.

- **Symptom:** The structural EKS teardown guard and the nightly FinOps sweeper were built, tested, and wired. A review found that they would never tear anything down once the customer-managed permissions boundary was applied.
- **Wrong first hypothesis:** The inline IAM policies of the teardown Lambdas grant `eks:DeleteCluster` and `kafka:DeleteCluster`, so the delete path is covered.
- **Root cause:** A role's effective permissions are the intersection of its inline policy and the permissions boundary. The boundary's Allow ceiling was written before EKS and the guard existed, and nobody widened it when they were added. It omitted `eks:*`, `kafka:*`, and `autoscaling:*`. Every delete call would therefore fail with AccessDenied at runtime. The Lambda catches the error, decides that nothing needs teardown, and publishes a healthy SNS summary. Meanwhile the EKS control plane and MSK Serverless would keep billing past the hard cap. A stale ceiling would defeat the guard that was built to replace procedural discipline.
- **Fix:** `eks:*`, `kafka:*`, and `autoscaling:*` joined the boundary ceiling. A regression test fails if any service that the teardown Lambdas call is missing from the ceiling. The IAM-escalation Deny statements are unchanged.
- **Lesson:** A ceiling that denies by omission acts as a second, invisible policy on every role. It must change in step with every new service the platform can start, or the cost safety check for that service stops working without any sign. Test the intersection as well as the inline grant, and assert that every service a teardown path deletes is inside the boundary.

## P39: Row-level security over single-column business keys is not multi-tenant isolation

**Tags:** SECURITY CORRECTNESS

**Status:** GROUNDED. The work ran from 2026-07-11 to 2026-07-13. The defect was reproduced locally, and the composite-key fix was implemented and verified locally against PostgreSQL 16, a local kind CDC stack, and serving-image checks. The transcripts are not included here. The live AWS migration and the rebuild of the derived stores have not run.

- **Symptom:** The row-level security drills pass. A session with no tenant reads zero rows, and tenant B cannot read tenant A's rows, so tenant isolation looks done. A pressure test then showed that two tenants sharing one Postgres still collide.
- **Wrong first hypothesis:** Adding `tenant_id`, `FORCE ROW LEVEL SECURITY`, and a fail-closed policy predicate to every tenant table is enough for co-tenancy.
- **Root cause:** Row-level security controls which rows a session can see, and it does not make keys unique per tenant. The tenant tables kept single-column business primary keys (`vessels.mmsi`, `watchlist.mmsi`, `sanctions_flags.id`). When tenant B upserts a key that tenant A already holds, `ON CONFLICT (mmsi) DO UPDATE` conflicts with A's row, which the policy hides from B. Postgres raises SQLSTATE 42501, and the API returns an unhandled 500. That error also tells B that another tenant holds the key, which is a covert channel. The tenant-private annotations, such as the watchlist reason and severity and the sanctions flags, were keyed only by MMSI, so tenants could overwrite and read each other's private data. The CDC read side had the same gap. Its Debezium consumer ignored tenants, its DynamoDB table was keyed on MMSI only, and its Redis cache was tenant-agnostic. Debezium bypasses row-level security, so the writer with the last LSN won and cross-tenant reads leaked.
- **Fix:** The tenant tables now use composite `(tenant_id, business_key)` primary keys with composite `ON CONFLICT` targets. An explicit transactional migration adds a sentinel backfill and row-preservation checks. The tenant dimension flows through the Debezium envelopes, the DynamoDB feature keys, `WatchlistLookup`, and the Redis keys. The migration and the runtime schema bootstraps share an advisory lock. Local PostgreSQL 16 tests proved same-MMSI isolation under row-level security, and two local containers built from the serving image read only their own tenant's values. A fresh local kind stack passed the local CDC smoke and all five end-to-end checks for Workflow 3. The live AWS cutover still needs an operator-run Postgres migration and a verified tenant-qualified rebuild of DynamoDB and Redis.
- **Lesson:** Row-level security is a visibility filter on top of a schema. It does not make a globally unique key unique per tenant, and it does not reach a CDC stream that reads the WAL directly. Multi-tenant isolation is a property of the keys and of every store in the pipeline. A drill that exercises only reads and writes without a tenant will certify an isolation model that a same-key upsert breaks.

## P40: Mutation testing on the shared working tree leaves stale bytecode in `__pycache__`

**Tags:** TOOLING

**Status:** GROUNDED. I observed it on 2026-07-11 while I fixed findings from an adversarial review sweep.

- **Symptom:** After the sweep, `test_tenant_drift` began to fail in isolation, against source that `git diff` showed was unchanged. `inspect.getsource` printed the correct function body, yet the imported function returned a wrong result on the same objects.
- **Wrong first hypothesis:** The cause was a real logic bug in `drifted_tenants`, or a stray uncommitted mutation left in the source.
- **Root cause:** The sweep's mutation-testing step edited source files in place to see whether a mutation survived, and then reverted them. The reverts restored the source cleanly. Python had already compiled `.pyc` bytecode from the mutated source into `__pycache__`, and its invalidation check did not trigger a recompile. The interpreter therefore loaded stale bytecode that no longer matched the correct source. `inspect.getsource` read the `.py` file and looked right, while the running code came from the stale `.pyc`.
- **Fix:** Clearing every `__pycache__` directory turned the full suite green. The recorded lesson says that future sweeps should run with `PYTHONDONTWRITEBYTECODE=1` or clear the cache on exit.
- **Lesson:** Any tool that rewrites source in place, such as mutation testing, a codemod, or an autofixer, can leave stale `.pyc` files that no longer match their source. A test that fails against source that reads correctly is the typical sign of this problem. When behavior contradicts the visible source, check the bytecode cache before the logic.

## P41: An optional empty Terraform value is still an invalid AWS API value

**Tags:** TOOLING

**Status:** GROUNDED. The episode happened on 2026-07-12 in a demonstration window on AWS (`infra/terraform/modules/kda_flink/main.tf`). The fix was verified in the same window.

- **Symptom:** The Kinesis Data Analytics v2 application failed during `terraform apply` with `ValidationException: Member must have length greater than or equal to 1`. The empty `quarantine_bucket` value was meant to disable quarantining on purpose.
- **Wrong first hypothesis:** An unrelated Flink application setting or a Terraform provider serialization bug was producing a malformed request.
- **Root cause:** Terraform still serialized `quarantine_bucket = ""` into the Flink property map. Kinesis Analytics v2 rejects every zero-length property-map value, and at the AWS API boundary an empty string differs from an absent optional property.
- **Fix:** The property map is now built with `merge()`, which adds `quarantine_bucket` only when it is nonempty. The Flink job already treats a missing property as disabled, so the change fixed the request without changing runtime behavior.
- **Lesson:** An optional value has two forms, absent and present but empty, and an external API may accept only one of them. Model omission explicitly at the infrastructure-code boundary, and do not assume that an empty string keeps its application meaning through every layer.

## P42: `.dockerignore` excluded the source tree that a nested Dockerfile needed

**Tags:** TOOLING

**Status:** GROUNDED. The episode happened on 2026-07-12 during an image build in the same window (`.dockerignore` and `cdc/consumer/Dockerfile`). The image rebuilt successfully in that window.

- **Symptom:** The first real build of the CDC consumer image failed at `COPY cdc ./cdc`. Docker reported that the source path was missing, although the directory existed in the repository.
- **Wrong first hypothesis:** The Dockerfile used the wrong build context, or the image-build command pointed at the wrong directory.
- **Root cause:** The repository-level `.dockerignore` excluded the whole `cdc` tree from the build context. The Connect image had never exposed the mistake, because its Dockerfile downloads a JAR and copies no local CDC source. The consumer Dockerfile imports the package, so it must copy it.
- **Fix:** The blanket `cdc` exclusion was removed, and a comment documents why that tree must stay in the context. The same carve-out already existed for another locally imported package.
- **Lesson:** A build-context filter is a shared dependency of every Dockerfile that uses the context. A successful sibling image proves nothing if it copies a different part of the repository, so test each real Dockerfile against the exact context it will receive.

## P43: Private DNS made one endpoint security group a VPC-wide dependency

**Tags:** NETWORKING SECURITY

**Status:** GROUNDED. The episode happened on 2026-07-12 during the live CDC deployment in the same window (`infra/terraform/modules/cdc_monitoring/main.tf`). After the live fix, the Debezium task retrieved its secret.

- **Symptom:** The Debezium Fargate task failed at startup with `ResourceInitializationError`. `GetSecretValue` timed out, although the task had the right Secrets Manager permission and outbound network access.
- **Wrong first hypothesis:** The secret ARN, the task execution-role policy, or the ECS secret injection was wrong.
- **Root cause:** The monitoring module created private-DNS interface endpoints for Secrets Manager and CloudWatch. Those DNS names resolve to the endpoint ENIs for every client in the VPC. The endpoint security group allowed port 443 only from the slot-lag Lambda's security group, so Debezium was routed to the endpoint and then blocked at ingress without a clear error.
- **Fix:** The endpoint security group now allows HTTPS ingress from the VPC CIDR. That matches the VPC-wide scope of private DNS and the convention in the other modules.
- **Lesson:** Private DNS changes the network path for the whole VPC and not only for the resource that created the endpoint. The endpoint security group must trust every intended caller in the VPC, or valid IAM requests will look like unexplained network timeouts.

## P44: MSK's data plane uses `kafka-cluster:*`, and its control plane uses `kafka:*`

**Tags:** SECURITY TOOLING

**Status:** GROUNDED. The episode happened on 2026-07-12 during the live CDC deployment in the same window (`infra/aws/harbormaster-permissions-boundary.json`). Boundary policy v2 was applied live, and the Connect worker then authenticated to MSK and joined its group.

- **Symptom:** The Connect worker crash-looped with `SaslAuthenticationException`, and ECS Exec was unavailable. Its task role held the expected MSK IAM and session-channel permissions.
- **Wrong first hypothesis:** The task-role policy, the MSK bootstrap configuration, or the IAM SASL client properties were incomplete.
- **Root cause:** The permissions boundary is an intersection, so any task-role allow outside the boundary acts as a deny. The boundary allowed `kafka:*`, which is the MSK control-plane namespace. It omitted `kafka-cluster:*`, which is the MSK IAM data-plane namespace. It also omitted `ssmmessages:*`, which ECS Exec needs for its channels.
- **Fix:** Both namespaces joined the boundary, which was applied as policy version v2. The worker then authenticated to MSK and joined its consumer group, and the live debugging channel became available.
- **Lesson:** AWS can split a service's permissions across similarly named IAM namespaces, and a permissions boundary must admit every namespace that the workload uses. Validate the effective permissions at the intersection, and do not rely on reading the task role alone.

## P45: An unquoted remote heredoc erased a secret-provider reference before validation

**Tags:** TOOLING SECURITY

**Status:** GROUNDED. The live symptom appeared on 2026-07-12 in the same window. The root cause was reproduced locally that day, and the fix was also verified locally that day (`cdc/connector/registration.py`, test `cdc/tests/test_connector_registration.py`). The transcript is not included here. This snapshot does not include evidence from any AWS retry of the corrected command.

- **Symptom:** Debezium connector validation reported an empty database password in separate attempts that used `${env:...}` and `${dir:...}` references. The ECS-injected secret existed, and the tmpfs file used by the `dir` attempt was non-empty.
- **Wrong first hypothesis:** Kafka Connect 3.7 resolved ConfigProvider references after `Connector.validate()`, or the providers in the Debezium image were broken at validation time.
- **Root cause:** The runbook sent the connector JSON through an unquoted remote Bash heredoc. Bash expanded `${env:...}` and `${dir:...}` as shell parameter expressions before `curl` ran. Kafka Connect therefore received an empty string and never saw a provider placeholder. The Kafka 3.7 source confirmed that the worker-side transformation happens before connector validation.
- **Fix:** The JSON is now base64-encoded locally, decoded inside the remote command, and posted with `--data-binary @-`. The regression test runs the real shell and HTTP transport and proves that the placeholder survives. A fresh local stack with Debezium 2.7 and Connect 3.7 reached connector and task state `RUNNING` and passed all five end-to-end checks for Workflow 3.
- **Lesson:** When a downstream parser appears to erase syntax, inspect every upstream interpreter first. Secret references are code-like strings, and the shell transport must preserve their bytes exactly. Encode the payload so that no quoting rule can change it.

## P46: A rolling ECS deployment leaves two plausible task ARNs

**Tags:** CONCURRENCY TOOLING

**Status:** GROUNDED. This was an operational finding from the 2026-07-12 window. This entry corrects the runbook, and no code path changed.

- **Symptom:** An ECS Exec check seemed to show that a just-deployed configuration change had not taken effect. After a successful apply, the inspected task still showed the old environment and behavior.
- **Wrong first hypothesis:** Terraform or ECS had ignored the new task definition, or the replacement task had started with a stale configuration.
- **Root cause:** The ECS rolling deployment ran the old and new tasks together for a while. A task ARN captured before the apply still identified a valid running task, but that task was the retiring revision. The later checks therefore inspected the retiring container.
- **Fix:** The runbook now re-fetches the current `RUNNING` task ARN after every apply and before any diagnostics.
- **Lesson:** In a rolling deployment, `RUNNING` shows that a task is alive and not that it is current. Treat task identity as temporary after every deployment, and re-fetch it before you collect diagnostic evidence.
