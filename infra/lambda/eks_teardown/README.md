# EKS teardown guard Lambda

This directory holds the source for the Lambda that `infra/terraform/modules/eks_teardown_guard` deploys. It is packaged exactly like `infra/lambda/teardown`, the nightly teardown. A `data "archive_file"` block zips this directory as it is, boto3 comes from the Lambda runtime, and nothing else is bundled.

The EKS control plane is the only compute surface in the project whose idle cost cannot be scaled to zero. It carries a fixed hourly charge whether or not any node or pod runs. Other parts of the project depend on an operator switching the feature off after a demo, and that step is procedural. This guard is structural. A recurring EventBridge Scheduler schedule re-evaluates the cluster's age on every run. Once `MAX_AGE_HOURS` (default 4) has elapsed, the guard force-destroys the node groups and then the cluster. The only exception is a cluster whose `KeepAliveUntil` tag holds a future ISO 8601 timestamp.

The decision function is pure, and `test_handler.py` tests its boundaries.

```python
should_teardown(created_at, keep_alive_until, now, max_age_hours)
```

The guard always fails toward teardown and never toward a control plane that runs forever. An absent, empty, or unparseable `KeepAliveUntil` tag grants no extension, and an unparseable `MAX_AGE_HOURS` falls back to the 4-hour default. EKS refuses `DeleteCluster` while node groups exist, so one run deletes the node groups first. The recurring schedule then finishes the cluster delete on a later run, and no invocation waits for it.

Run the tests locally. They need no AWS account and no credentials.

```bash
uv run --frozen python -m pytest infra/lambda/eks_teardown/test_handler.py -q
```
