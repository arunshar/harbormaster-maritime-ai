<h1 align="center">Harbormaster: Maritime Anomaly Detection from AIS to Analyst Review</h1>

<p align="center">
  <a href="https://github.com/arunshar/harbormaster-maritime-ai/actions/workflows/ci.yml"><img src="https://github.com/arunshar/harbormaster-maritime-ai/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/arunshar/harbormaster-maritime-ai" alt="License"></a>
  <img src="https://img.shields.io/python/required-version-toml?tomlFilePath=https%3A%2F%2Fraw.githubusercontent.com%2Farunshar%2Fharbormaster-maritime-ai%2Fmain%2Fpyproject.toml" alt="Python version">
</p>

I built Harbormaster as a personal project to connect raw vessel position reports to an analyst's review decision. Ships broadcast AIS (Automatic Identification System) position reports, and the system keeps recent history for each vessel. It scores unusual movement such as reporting gaps, implausible speed, and departures from common shipping lanes. When a review rule applies, it saves a case that an analyst can open, judge, and label. The design splits this work into five workflows, and each workflow has its own trigger and its own schedule.

The code, the tests, and the Terraform modules in this repository are real, and the unit suite runs in CI without cloud credentials. Parts of the system ran on AWS during bounded demonstration windows. That AWS environment is no longer running, and this repository deploys nothing by default. Pi-DPM is the project's diffusion model for vessel trajectory windows. Candidate V3 is a Pi-DPM model candidate that was benchmarked offline, and it is not deployed. Production V1 is a planned single-region pilot that [an architecture decision record (ADR)](docs/adr/ADR_PRODUCTION_V1_ECS_FRONT_DOOR.md) describes, and it is not accepted. The model benchmark in this README used constructed labels, so its numbers do not measure operational accuracy. [docs/HONESTY.md](docs/HONESTY.md) lists what is real, what is simulated, and what was never built.

## Who this is for

- **Engineers** should run [Quick start](#quick-start) first and then read [The whole system](#the-whole-system) and the five workflows. Workflow 4 marks its assumed integration, and Workflow 5 names the parts that this snapshot does not contain. [What is not built](#what-is-not-built) lists every gap.
- **Researchers** in trajectory anomaly detection should read [Workflow 4](#workflow-4-build-reusable-shipping-lane-context), [Workflow 5](#workflow-5-run-a-separate-model-job-and-verify-its-result), and [Evaluation](#evaluation).
- **Reviewers** should read this introduction first and then read [Evaluation](#evaluation) and [What is not built](#what-is-not-built). Each workflow also names its main failure and the fix. The P numbers in those sections point to entries in [docs/WAR_STORIES.md](docs/WAR_STORIES.md).

## Table of contents

1. [Quick start](#quick-start)
2. [The whole system](#the-whole-system)
3. [Workflow 1: Check incoming vessel reports](#workflow-1-check-incoming-vessel-reports)
4. [Workflow 2: Save an analyst review](#workflow-2-save-an-analyst-review)
5. [Workflow 3: Keep registry copies and cached answers current](#workflow-3-keep-registry-copies-and-cached-answers-current)
6. [Workflow 4: Build reusable shipping-lane context](#workflow-4-build-reusable-shipping-lane-context)
7. [Workflow 5: Run a separate model job and verify its result](#workflow-5-run-a-separate-model-job-and-verify-its-result)
8. [Evaluation](#evaluation)
9. [What is not built](#what-is-not-built)
10. [Code tree](#code-tree)
11. [Diagrams](#diagrams)
12. [Reused code](#reused-code)
13. [Further reading](#further-reading)
14. [License](#license)

## Quick start

You need [uv](https://docs.astral.sh/uv/) and Python 3.11 or newer. CI uses Python 3.12. These commands install the locked dependencies and run the same lint and test steps as CI:

```bash
git clone https://github.com/arunshar/harbormaster-maritime-ai.git
cd harbormaster-maritime-ai
uv sync --frozen --extra dev --extra serving-runtime --extra ingestor --extra lake --extra mlops --extra pidpm-demo
uv run --frozen ruff check serving streaming cdc lake mlops tests scripts infra/lambda
uv run --frozen pytest -q --ignore-glob='tests/e2e/*'
```

On this snapshot, the test command reported 821 passed tests and 12 skipped tests, with no failures. The unit tests need no AWS credentials, and the tests that need a live Postgres database skip without one. Every command in the workflow sections below assumes that you ran the `uv sync` command above once.

The `tests/e2e/` directory holds acceptance and helper tests, and CI does not run it. Some of those tests need a live stack and skip without one. The `Makefile` lists the targets that start a local stack.

## The whole system

<a href="assets/diagrams/overview.png"><img src="assets/diagrams/overview.png" width="100%" alt="Integrated target design with the five workflows and the stores they share"></a>

*This overview shows the integrated target design, and it does not show a deployed system. The same drawing is available as a vector file, [overview.svg](assets/diagrams/overview.svg), and its editable source is [overview-merged-system.excalidraw](assets/diagrams/src/overview-merged-system.excalidraw).*

The overview places all five workflows on one page, with the requirements, the core entities, and the API on the left. It numbers the workflows 1 to 5 and draws Workflow 2 at the top, above Workflow 1. The per-workflow diagrams below label them W1 to W5. The boxes name logical responsibilities, so two boxes can share one deployed service. For example, the review handlers and the scoring handlers both run inside one FastAPI application.

Each workflow starts on its own trigger, so opening the dashboard does not start all five. Workflow 1 runs on every admitted vessel report, and Workflow 2 runs when an analyst opens a case. Workflow 3 runs on each committed registry change, and Workflow 4 runs on an operator-admitted historical batch. Workflow 5 runs when a client submits a model job.

The workflows share a few stores. The scoring service in Workflow 1 reads registry context from Workflow 3. It also reads a lane reference in the format that Workflow 4 is meant to publish, and this snapshot ships a synthetic demo reference in its place. The history store from Workflow 1 is meant to feed Workflow 4 through a preparation bridge that remains assumed integration. The review cases from Workflow 1 are the cases that Workflow 2 shows. Three dashed links in the overview mark assumed integration. They run from the History Store to Dataset Preparation, from the Release Publisher to the Lane Reference, and from the Lane Reference to the Scoring Service. The fourth dashed link, labeled Release, marks the operator's model release, and the diagram note says the operator controls that release separately from requests. Diagrams W1 to W4 draw every link solid, and their notes name the assumed parts. The overview and W5 draw the jobs service, the dispatch worker, and the result collector with solid lines, but this snapshot does not contain them. The dashed rectangles in the overview and in W5 only group related parts. They do not mark assumed integration the way a dashed link does.

## Workflow 1: Check incoming vessel reports

An AIS provider sends a vessel position report. In this snapshot the only runnable source is a replay of a synthetic fixture, because the ingestor refuses live mode until a licensed live source is chosen. A seeded generator in `streaming/replay/generate.py` builds that fixture, and it contains no AISStream or other third-party AIS data. Each AIS report carries an MMSI (Maritime Mobile Service Identity), which is a nine-digit number that identifies the vessel. The MMSIs, positions, and anomalies in the fixtures are synthetic, and the MMSIs are not linked to any vessel's real track.

A Fargate ingestion service parses and normalizes the report, and Amazon Kinesis holds it until the stream processor reads it. An Apache Flink job keeps recent history for each vessel, computes movement features, and computes P_phys. P_phys is a plausibility score from 0 to 1. It equals 1 when a vessel at the 25-knot cap could reach the new position in time, and it falls as the required speed rises. Reports with P_phys of at least 0.3 go to a FastAPI scoring service at `POST /v1/score-ais`, so every ordinary report is scored. A report below 0.3 needs more than about 83 knots, and the job does not send it. That report still stays in the vessel's history, so the next report's history includes it. The scorer runs gap, speed, and corridor-deviation detectors, and it saves a review case in the PostgreSQL `hitl_queue` table when a review rule applies. HITL stands for human-in-the-loop, and Workflow 2 describes that review step.

<img src="assets/diagrams/w1.png" width="100%" alt="Workflow 1: AIS provider, ingestion service, event stream, stream processor, scoring service, and review database, with the history store, feature store, and reference context below">

*The W1 diagram shows reports moving from the AIS provider through ingestion, the event stream, and the stream processor to the scoring service and the review database. The AWS parts of this path ran only in bounded demonstration windows. In this snapshot, a synthetic fixture replay stands in for the AIS provider, because live ingestion is disabled.*

The diagram also shows two side branches. Firehose writes raw observations to the history store in S3, and the stream processor writes derived features to a DynamoDB feature store. The scorer uses its loaded reference context and does not read the feature store.

**What went wrong:** During the first live run, the review queue stayed empty even though the pipeline reported no errors. The Flink job sent precomputed features under a `features` key, but the scoring API expected the vessel identifier, the current fix, and recent history. The API rejected every request with HTTP 422, and a best-effort exception handler hid those rejections ([war story P15](docs/WAR_STORIES.md#p15-a-precomputed-feature-payload-silently-drifted-from-the-real-serving-schema)).

**How I fixed it:** The Flink job now sends `{mmsi, fix, history}` in the exact shape the API expects. That fix exposed a second bug. The planner adds the gap detector only when a request carries at least three history points, but Flink kept only one prior fix. The job now keeps a rolling window of five fixes, so the gap detector runs ([war story P16](docs/WAR_STORIES.md#p16-a-deterministic-planner-silently-skips-a-detector-below-its-history-threshold)).

```python
# streaming/flink/transforms.py (the request body sent to the scorer)
    return {
        "mmsi": mmsi,
        "fix": _fix_dict(fix),
        "history": [_fix_dict(f) for f in (history or [])],
    }
```

```python
# streaming/flink/job.py (per-vessel keyed state keeps the last five fixes)
        history_json = self._history.value()
        history = _history_from_json(history_json) if history_json else []
        prev = history[-1] if history else None
        feats: WindowFeatures = window_features(fix, prev)
        self._history.update(_history_to_json((history + [fix])[-HISTORY_WINDOW:]))
```

The ingestor and the Flink job need Kinesis and Managed Flink, so no local command runs the whole Workflow 1 path. The local commands below run the stream-processing and scoring tests, or they start the scoring API by itself on port 8000:

```bash
uv run --frozen pytest -q streaming/flink/tests serving/tests/test_score_ais.py
PYTHONPATH=serving uv run --frozen uvicorn app.main:app --port 8000
```

Keep the API running in its own terminal, and send the example request below from a second terminal. The request describes a vessel that stopped reporting for 178 minutes. The scorer returns an `abnormal_gap` reason with `hitl_required: true`, and the case then appears in the pending review list:

```bash
curl -s -X POST localhost:8000/v1/score-ais -H 'content-type: application/json' --data @serving/examples/gap_request.json
curl -s localhost:8000/v1/hitl/pending
```

In a bounded AWS window, a 36,000-request load test ran against `POST /v1/score-ais` at 10 requests per second for one hour. It returned 35,999 HTTP 200 responses and one 503, with client p95 142.751 ms and p99 240.283 ms. The test repeated one fixed request, so it measures serving capacity and says nothing about model quality. The harness is `scripts/loadtest_signed_serving.py`. It refuses to run until you set `HM_LOADTEST_ACCOUNT_ID` to your own account ID, so no account ID needs to be written into the repository.

## Workflow 2: Save an analyst review

An analyst opens pending review cases in a Streamlit dashboard and reads the evidence. The analyst then records one of three labels, which are `correct`, `incorrect`, and `ambiguous`. This is the HITL review step. The dashboard calls `GET /v1/hitl/pending` and `POST /v1/feedback` on the FastAPI application. The review service saves the label and the reviewer name, and it keeps the original automated score unchanged. Feedback is stored for later comparison, and it does not retrain the model. The module `mlops/preference_builder.py` can turn `incorrect` labels into offline preference records, and no code path retrains or updates a model from them.

<img src="assets/diagrams/w2.png" width="100%" alt="Workflow 2: analyst, review dashboard, application API, review service, review database, and the scoring service that creates review cases">

*The W2 diagram shows the analyst's path from the review dashboard through the application API and the review service to the review database. The scoring service from Workflow 1 creates the review cases that this path reads.*

**What went wrong:** A code audit found that the AWS task definition never injected the database credential that the review backend needed. The backend had quietly fallen back to an in-memory stub on real infrastructure, so saved reviews lived only in process memory. A second risk involved the update itself, because a feedback write that matched the wrong row could change another case or overwrite the automated score.

**How I fixed it:** The task definition now injects the credential. The backend now stops at startup on a schema, migration, or row-level security failure. It still uses process memory without a warning when no database connection is configured, and it falls back to memory with a warning when the database is unreachable at startup. The feedback write updates only the `label` and `reviewer` columns for the matching trace ID. An unknown trace ID returns the error code `harbormaster.hitl_trace_not_found`.

```python
# serving/app/hitl.py
    async def label(self, payload: FeedbackIn) -> int:
        async with self._pool.acquire() as conn:
            status = await conn.execute(
                "UPDATE hitl_queue SET label=$1, reviewer=$2 WHERE trace_id=$3",
                payload.label,
                payload.reviewer,
                payload.trace_id,
            )
            if status.endswith(" 0"):
                raise HitlTraceNotFound("no queued review for trace_id", trace_id=payload.trace_id)
            row = await conn.fetchrow("SELECT count(*) AS n FROM hitl_queue WHERE label IS NULL")
        return int(row["n"])
```

Run the review tests, or start the dashboard against a running API:

```bash
uv run --frozen pytest -q serving/tests/test_hitl.py serving/frontend/tests
SERVING_URL=http://localhost:8000 uv run --frozen --extra console streamlit run serving/frontend/console.py
```

The Postgres-backed test in `serving/tests/test_hitl.py` skips unless `HM_TEST_PG_DSN` points to a live Postgres database.

## Workflow 3: Keep registry copies and cached answers current

An authorized change to a vessel, watchlist, or sanctions record commits in PostgreSQL first. Change data capture (CDC) carries the change onward. Debezium runs inside Kafka Connect and reads the committed change through logical decoding. It then publishes an event to Apache Kafka. A registry worker updates the DynamoDB read store and removes the stale Redis cache entry, and it also appends a row to an Iceberg audit table. A later scoring request therefore sees the new context. The review table `hitl_queue` stays outside this change stream.

<img src="assets/diagrams/w3.png" width="100%" alt="Workflow 3: registry database, change capture, change stream, registry worker, cache, read store, audit store, and the scoring service that reads the cache and the read store">

*The W3 diagram shows a committed registry change moving through change capture and the change stream to the registry worker. The worker invalidates the cache, updates the read store, and appends to the audit store. The scoring service reads the cache and the read store.*

**What went wrong:** In an AWS window, Kafka Connect received an empty database password even though the connector configuration held a secret reference. The runbook sent the JSON through an unquoted remote Bash heredoc. Bash expanded the placeholder into an empty string before Kafka Connect saw it ([war story P45](docs/WAR_STORIES.md#p45-an-unquoted-remote-heredoc-erased-a-secret-provider-reference-before-validation)). A second problem involved tenant ownership. Single-column business keys let two tenants in one database collide on a key and overwrite private annotations ([war story P39](docs/WAR_STORIES.md#p39-row-level-security-over-single-column-business-keys-is-not-multi-tenant-isolation)).

**How I fixed it:** The registration code now base64-encodes the JSON locally and decodes it inside the remote command, so the placeholder reaches Kafka Connect unchanged. The tenant tables now use composite `(tenant_id, business_key)` keys, and tenant identity flows through the Debezium envelopes, the DynamoDB keys, and the Redis keys. The worker also keeps a per-key LSN (log sequence number) guard, so an older or duplicate event cannot replace newer state ([war story P10](docs/WAR_STORIES.md#p10-duplicate-cdc-events-after-a-consumer-restart-at-least-once-delivery-and-a-non-idempotent-sink)). This snapshot does not include the evidence from any later AWS run of the connector, so this repository claims no end-to-end change-capture run on AWS.

```python
# cdc/connector/registration.py
    encoded = encode_connector_config(body)
    connector_name = str(body["name"])
    url = f"{connect_url.rstrip('/')}/connectors/{connector_name}/config"
    script = (
        f"printf '%s' {shlex.quote(encoded)} | base64 -d | "
        "curl --fail-with-body --silent --show-error --request PUT "
        "--header 'Content-Type: application/json' --data-binary @- "
        f"{shlex.quote(url)}"
    )
    return f"/bin/bash -c {shlex.quote(script)}"
```

```python
# cdc/sinks/dynamo.py (the LSN guard is a DynamoDB conditional write)
CONDITION_EXPRESSION = "attribute_not_exists(last_applied_lsn) OR last_applied_lsn < :lsn"
```

Run the change-capture unit tests without any stack:

```bash
uv run --frozen pytest -q cdc/tests
```

The five end-to-end checks in `tests/e2e/test_phase2.py` need a local stack with Kafka, Debezium, Postgres, Redis, and DynamoDB Local. That stack needs Docker, kind, and kubectl. The registry worker also needs the `cdc` extra, which the Quick start command does not install. Stop the Workflow 1 API first, because `make serve-run-cdc` also uses port 8000. Then run these commands in a first terminal:

```bash
uv sync --frozen --extra dev --extra serving-runtime --extra ingestor --extra lake --extra mlops --extra pidpm-demo --extra cdc
make cdc-up          # start the local stack
make cdc-smoke       # register the Debezium connector (run once)
```

In a second terminal, start the registry worker and keep it running:

```bash
make cdc-consumer
```

In a third terminal, start the scoring API against the local stack and keep it running:

```bash
make serve-run-cdc
```

Then run the checks in the first terminal:

```bash
make cdc-e2e
```

On a fresh local stack, the five checks ran in 36.41 s. They covered flag-to-scored latency, duplicate-free full replay, Debezium restart recovery, delete propagation, and replication-slot lag alerting.

## Workflow 4: Build reusable shipping-lane context

An operator admits a batch of historical AIS tracks. A PySpark job on EMR Serverless checks the records with a Great Expectations gate and standardizes their positions. The lane builder simplifies each vessel's track with the Ramer–Douglas–Peucker (RDP) algorithm. It then groups the remaining turn points across vessels into waypoints with HDBSCAN, and it counts the connections between those waypoints. Apache Iceberg stores the resulting lane nodes and edges, with AWS Glue as the catalog. The scoring service later compares a vessel's position with a lane reference. A departure from a common lane gives the analyst a reason to look closer, but it does not show by itself that a vessel did anything wrong.

<img src="assets/diagrams/w4.png" width="100%" alt="Workflow 4: history store, dataset preparation, lane builder, lane tables, release publisher, lane reference, and scoring service">

*The W4 diagram shows historical tracks moving from the history store through dataset preparation and the lane builder into lane tables. A lane reference then carries the result to the scoring service. The Release Publisher, the serving bridge, and the preparation bridge from the history store are assumed integration.*

**What went wrong:** The first live EMR backfill failed four times in a row, and local unit tests had caught none of the four causes. The job's IAM role could not read its own code in S3 ([war story P17](docs/WAR_STORIES.md#p17-the-jobs-own-iam-role-never-had-permission-to-read-its-own-code)). The EMR image ran Python 3.9, which rejects `zip(strict=...)` ([war story P18](docs/WAR_STORIES.md#p18-a-python-310-idiom-breaks-on-the-emr-runtimes-python-39)). A Spark output schema disagreed with the function's real return type ([war story P19](docs/WAR_STORIES.md#p19-a-pure-functions-own-docstring-predicted-the-bug-in-the-spark-wiring-around-it)), and the Glue catalog client needed its own region setting ([war story P20](docs/WAR_STORIES.md#p20-a-catalog-client-needs-its-own-region-even-when-every-other-aws-client-in-the-process-has-one)).

**How I fixed it:** Each bug received a narrow fix. I added an IAM statement for the code prefix, and I removed the `strict=` argument to `zip`, which Python 3.9 does not accept. I also wrote a separate output schema and set explicit catalog region properties. The general lesson is to check the real target runtime and its permissions before trusting a green local test run.

```python
# lake/backfill/transforms.py
def derive_corridor_graph(
    df: pd.DataFrame, *, epsilon_m: float = 200.0, min_cluster_size: int = 2
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run the full lane pipeline on canonicalized positions.

    The steps are per-vessel RDP simplification, cross-vessel HDBSCAN waypoint
    clustering, and edge derivation. The function returns
    (corridor_graph_nodes, corridor_graph_edges)."""
    simplified = simplify_all_tracks(df, epsilon_m=epsilon_m)
    nodes = cluster_waypoints(simplified, min_cluster_size=min_cluster_size)
    edges = derive_edges(simplified, nodes)
    return nodes, edges
```

The transforms are plain pandas, NumPy, and scikit-learn, so they run without Spark or AWS. Run the lake tests, or run the quality gate, the transforms, and a real Iceberg write against the committed synthetic fixture:

```bash
uv run --frozen pytest -q lake/tests
uv run --frozen python scripts/lake_backfill_smoke.py
```

The committed fixture holds one vessel, so the smoke run reports an empty lane graph by design. The multi-vessel clustering case is covered by `lake/tests/test_backfill_transforms.py`.

Three parts in the diagram are assumed integration, and this repository does not contain them. They are the preparation bridge from the Firehose JSON archive to backfill Parquet, the Release Publisher, and the serving bridge. The checked-in lane reference in `serving/app/artifacts/corridors.json` is a small synthetic demo graph for one region near New York and New Jersey. A licensed historical AIS extract was used locally and is not redistributed. The demo lane graph does not come from that extract. See [docs/corridor-detector.md](docs/corridor-detector.md) for the corridor detector.

## Workflow 5: Run a separate model job and verify its result

Pi-DPM is the project's diffusion model for vessel trajectory windows, and its scorer code lives in a separate repository. In the target design, a client submits an eligible trajectory window and receives a job reference right away. The input goes to an S3 input store. A SageMaker asynchronous inference endpoint scores it in the background, and it writes the output to an S3 output store. In the design, a result collector checks that the output belongs to the right request and the right model before the client can read it. The endpoint can scale from zero instances and back to zero.

<img src="assets/diagrams/w5.png" width="100%" alt="Workflow 5: job client, jobs service, dispatch worker, input store, model endpoint, model release, output store, and result collector">

*The W5 diagram shows the planned model-job path. The jobs service, the dispatch worker, and the result collector drawn there are not in this public snapshot. The diagram's notes about the internal service, publication before dispatch, and dispatch retries describe that missing code, so this repository cannot confirm them.*

This snapshot holds part of that path. It contains the Docker model container on SageMaker's `/ping` and `/invocations` contract, the SageMaker async Terraform module, and an async client in the scorer. When the async call fails or times out, the scorer falls back to an analytic estimate, so an outage lowers quality and scoring continues. The job HTTP routes remain proposed.

**What went wrong:** The first SageMaker deploy failed because the container had no ENTRYPOINT to receive SageMaker's `serve` argument ([war story P22](docs/WAR_STORIES.md#p22-a-container-with-no-entrypoint-cannot-answer-the-platforms-invocation-convention)). Later, one autoscaling alarm could never fire because its metric had a missing dimension, and the paired wake-from-zero alarm had an extra one (war stories [P25](docs/WAR_STORIES.md#p25-an-audit-finding-on-paper-and-a-live-verified-fix-are-different-claims) and [P26](docs/WAR_STORIES.md#p26-the-same-class-of-bug-survived-the-targeted-audit-fix-in-a-second-alarm)). These three bugs show that a model job needs a live check of its output, because a clean build and a successful acknowledgement can both hide a broken job.

**How I fixed it:** The image gained an entrypoint that starts the server whatever argument SageMaker passes. Both alarms now use the dimensions that SageMaker actually publishes, and a live run confirmed scale-in to zero and scale-out from zero.

```python
# mlops/pidpm_container/demo/entrypoint.py
def main() -> None:
    # The argv list is fixed. It uses no shell and takes no user-controlled path or argument.
    os.execv(  # nosec B606
        "/usr/local/bin/gunicorn",
        [
            "/usr/local/bin/gunicorn",
            "--bind",
            "0.0.0.0:8080",
            "--workers",
            "1",
            "server:app",
        ],
    )
```

Run the async client and demo container tests:

```bash
uv run --frozen pytest -q serving/tests/test_pidpm_client.py mlops/pidpm_container/demo/tests
```

The model returns a numeric score, and a frozen threshold turns that score into a flag. A reviewed label needs separate evidence, so the score, the flag, and the label are three different objects. The live endpoint served a labeled demo stand-in model and never a trained checkpoint.

## Evaluation

| What was measured | Where it ran | Result |
| --- | --- | --- |
| Unit and integration suite, the CI command | Local, Python 3.12, no cloud credentials | 821 passed, 12 skipped, 0 failed |
| Line and branch coverage over `serving`, `streaming`, `cdc`, `lake`, `mlops` | Local, same command with `--cov` | 83.72% (the configured floor is 80%) |
| Lint, `ruff check` on the CI paths | Local | All checks passed |
| Workflow 1 load test on `POST /v1/score-ais`, 36,000 requests at 10 requests per second for one hour | Bounded AWS window | 35,999 HTTP 200 and one 503, client p95 142.751 ms, p99 240.283 ms |
| Workflow 3 five end-to-end change-capture checks | Fresh local stack | Passed in 36.41 s |
| Controlled benchmark of Candidate V3, a Pi-DPM model candidate (code not in this snapshot) | Offline, constructed labels | 99.1447% precision and 100% proxy recall under constructed labels (not operational accuracy) |
| Physics baseline on the same benchmark | Offline, constructed labels | AUROC (area under the receiver operating characteristic curve) 1.0 |

**Limits:**

- The 12 skipped tests need a live Postgres database through `HM_TEST_PG_DSN`. CI does not run coverage, so the 83.72% figure comes from a local run.
- The load test repeated one fixed request, so it measures serving capacity and not model quality.
- The controlled benchmark scored Candidate V3, a Pi-DPM model candidate whose code lives in a separate repository. It did not score the detectors in this repository.
- The controlled benchmark built its own labels by applying constructed changes to trajectory windows. Most benchmark windows were constructed positives, and that raises precision.
- Proxy recall is recall measured against those constructed labels. The 99.1447% precision and 100% proxy recall are therefore not operational accuracy.
- A physics baseline also reached AUROC 1.0 on that benchmark, so the benchmark shows no learned advantage for the model.
- No analyst produced the benchmark labels, and no field false-alert rate has been measured.
- This snapshot does not contain the controlled benchmark code, so it cannot reproduce these figures.

## What is not built

- The AWS demonstration environment is no longer running, and this repository deploys nothing by default.
- Candidate V3 is not deployed, and Production V1 is not accepted.
- Live AIS ingestion is disabled in this snapshot, so the ingestor replays only the synthetic fixture.
- The Flink job has no event-time windows or watermarks, so it does not reconcile late or out-of-order reports ([ADR 0001](docs/adr/0001-streaming-per-event-realization.md)).
- The scorer does not read the feature store before it scores a report.
- The Flink gate drops reports that need more than about 83 knots. The scorer would flag jumps up to 12 times the 25-knot cap as implausible speed, so the streaming path never scores the jumps between about 83 and 300 knots.
- The rendezvous finder in `serving/app/agents/rendezvous_finder.py` has tests, but no API route calls it, because it needs a multi-vessel endpoint that does not exist.
- The Bedrock explainer in `serving/app/bedrock_explainer.py` is disabled by default, and no route calls it.
- The review queue still uses process memory when no database connection is configured or when PostgreSQL is unreachable at startup.
- The API accepts the reviewer name from the caller and does not bind it to an authenticated identity.
- This snapshot does not include evidence from any AWS change-capture run after the heredoc fix, so this repository claims no end-to-end change-capture run on AWS.
- The composite-key tenant migration has not run on AWS.
- Cache invalidations and audit records can repeat after a redelivery, so the system does not claim exactly-once effects across every destination.
- The lane Release Publisher and the bridge from Firehose JSON to backfill Parquet are assumed integration.
- The serving loader does not validate a lane release version.
- HDBSCAN collects each batch onto the Spark driver, so each batch must fit in driver memory.
- The model-job routes are proposed, and the jobs service, the dispatch worker, and the result collector are not in this snapshot.
- The full Pi-DPM container in `mlops/pidpm_container/Dockerfile` is illustrative, because its scorer code lives in a separate repository.
- The project has no consensus protocol and no sharded query router ([ADR 0004](docs/adr/0004-no-consensus-no-sharded-query-router.md)).
- Argo CD rollout, a change-event schema registry, API idempotency keys, and a circuit breaker with jittered retry exist only as design notes.
- CI lints the Lambda handlers in `infra/lambda/`, but it does not run their tests.
- CI does not run checkov, although `infra/terraform/.checkov.baseline` ships with the Terraform modules.

## Code tree

```text
harbormaster-maritime-ai/
├── streaming/          # Workflow 1 intake: AIS ingestor, replay generator, synthetic replay fixtures, PyFlink job
│   ├── ingestor/       #   parses and normalizes reports before they enter Kinesis
│   ├── features/       #   per-vessel movement features, including P_phys
│   ├── fixtures/       #   the synthetic AIS replay fixture and its checksum
│   ├── replay/         #   builds the synthetic AIS replay fixture
│   └── flink/          #   per-vessel keyed state, P_phys gate, request to the scorer
├── serving/            # FastAPI scoring and review service
│   ├── app/            #   detectors, planner, HITL (human-in-the-loop) review queue, registry lookups, async model client
│   ├── examples/       #   an example scoring request that creates a review case
│   └── frontend/       #   Streamlit analyst console (Workflow 2)
├── cdc/                # Workflow 3: schema, Debezium connector config, LSN-guarded worker, sinks, slot-lag monitor
├── lake/               # Workflow 4: quality gate, PySpark backfill, RDP and HDBSCAN lane builder, Iceberg helpers
├── mlops/              # Workflow 5 and model lifecycle: model containers, promotion, drift, holdout and canary gates
│   ├── pidpm_container/ #  Workflow 5 model container on the SageMaker /ping and /invocations contract, with the demo stand-in in demo/
│   └── route_optimizer/ #  a CPU-only PPO (proximal policy optimization) experiment outside the five workflows
├── bench/              # latency benchmark for the deterministic scorer (it reports no accuracy numbers)
├── deploy/             # kind manifests for the local CDC stack and Kubernetes serving manifests
├── infra/
│   ├── terraform/      #   modules that the demonstration windows applied (nothing deploys by default)
│   ├── lambda/         #   teardown, EKS teardown guard, drift-watch, and slot-lag Lambda handlers with their own tests
│   └── aws/            #   bootstrap script and IAM permissions-boundary policy
├── hpc/                # template for the off-cloud training job manifest
├── scripts/            # smoke tests, drills, load-test harnesses, packaging helpers
├── tests/e2e/          # acceptance and helper tests (CI does not run this directory)
├── docs/               # workflows, architecture, design decisions, ADRs, war stories, honesty rules
└── assets/diagrams/    # overview and per-workflow diagrams, plus the Excalidraw source
```

File names, Makefile targets, and docs use build-phase numbers from the project's history. The section comments in the Makefile map each phase number to its work. Phase 1 built the Workflow 1 serving and streaming slice, and Phase 2 built the Workflow 3 change-capture plane. Phase 3 built the lake and promotion work behind Workflows 4 and 5. Phase 4 added drift checks and the offline preference builder, and Phase 5 added tenant isolation and the temporary EKS path. In the diagrams, W1 to W5 mean Workflows 1 to 5. Labels such as W1 to W4 and Wave 3 also appear in code comments, Terraform variable descriptions, and test docstrings. In those places they name demonstration windows and review rounds, and they do not refer to the workflows.

## Diagrams

The diagrams are Excalidraw drawings in [`assets/diagrams/`](assets/diagrams/). The overview appears above as `overview.png`, and its text matches the vector export `overview.svg`. Its editable source is [`assets/diagrams/src/overview-merged-system.excalidraw`](assets/diagrams/src/overview-merged-system.excalidraw), which you can open at excalidraw.com. The per-workflow diagrams `w1.png` to `w5.png` are PNG exports and have no source file in this repository. These five workflow diagrams are the exact versions used in the project presentation.

## Reused code

Some modules come from three earlier repositories that I wrote. GeoTrace-Agent and MIRROR carry the MIT License with the same copyright holder as this project. I hold the copyright in the earlier reinforcement-learning repository too, and I release the parts reused here under this project's MIT License. None of the three source repositories is public. Most reused files identify their source repository in their header. Three agent modules in `serving/app/agents/` come from GeoTrace-Agent. `validator.py` is unchanged, so it carries no source header. `space_time_reasoner.py` and `rendezvous_finder.py` differ only in imports, type annotations, one docstring line, and a provenance sentence in each module docstring.

- The geometry kernel, the geometry models in `serving/app/models.py`, the error-code shape in `serving/app/errors.py`, and the gap, validation, rendezvous, and space-time reasoning agents in `serving/app/` come from GeoTrace-Agent. The planner, the corridor detector, and the `/v1/score-ais` route are new.
- The adaptive KL (Kullback–Leibler) controller and the PPO structure in `mlops/route_optimizer/` come from the earlier reinforcement-learning repository. The reward weights in `mlops/preference_builder.py` and the logging adapter in `mlops/wandb_adapter.py` come from the same repository.
- The drift and holdout metrics in `mlops/drift.py` and `mlops/holdout_gate.py` come from MIRROR. The Kolmogorov–Smirnov p-value in `mlops/drift.py` is a new implementation.

### Method references

The scoring agents and the Workflow 5 target model build on published methods. I wrote the first three papers below with co-authors, and the last method comes from Hägerstrand's time geography.

- The gap detector in `serving/app/agents/gap_detector.py` builds on STAGD and Dynamic Region Merge from Sharma, Ghosh, and Shekhar, "Physics-based Abnormal Trajectory-Gap Detection," ACM TIST, 2024.
- The rendezvous finder in `serving/app/agents/rendezvous_finder.py` builds on TGARD and DC-TGARD from Sharma, Gupta, Ghosh, and Shekhar, "Towards a Tighter Bound on Possible-Rendezvous Areas," ACM SIGSPATIAL, 2022.
- Pi-DPM, the target model for Workflow 5, follows Sharma, Yang, Farhadloo, Ghosh, Jayaprakash, and Shekhar, "Towards Physics-informed Diffusion for Anomaly Detection in Trajectories," ACM SIGSPATIAL GeoAnomalies, 2025.
- The space-time prism in `serving/app/components/space_time_prism.py` comes from Hägerstrand's time geography.

## Further reading

- [docs/WORKFLOWS.md](docs/WORKFLOWS.md) explains the five workflows in more detail, with components and code paths.
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) describes the training plane, the serving plane, and the promotion boundary between them.
- [docs/SYSTEM_DESIGN_DECISIONS.md](docs/SYSTEM_DESIGN_DECISIONS.md) maps each design decision to its pattern and records what is built.
- [docs/WAR_STORIES.md](docs/WAR_STORIES.md) records 38 grounded entries (P9 to P46) from drills, live runs, reviews, and tests. It also records 8 anticipated failure modes (P1 to P8) that no run has observed. Each entry gives the symptom, the wrong first hypothesis, the root cause, the fix, and the lesson.
- [docs/adr/](docs/adr/) holds the architecture decision records.
- [docs/HONESTY.md](docs/HONESTY.md) sets the rules for describing this project.
- Some code comments cite private planning notes under `docs/phases/`, `docs/runbooks/`, and `docs/drills/`. Those notes are not part of this repository.

## License

This project is released under the [MIT License](LICENSE). Copyright (c) 2026 Arun Sharma. The font subsets embedded in `assets/diagrams/overview.svg` keep their own licenses, as [NOTICE](NOTICE) explains.
