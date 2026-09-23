# envs/demo

The `demo` environment is the short-lived, stand-up-then-tear-down variant of
Harbormaster used to show the platform live (for example during a walkthrough)
without leaving anything running afterward. This repository does not ship a
full `demo` root. This directory documents the intended shape, so the naming
and conventions stay consistent with `envs/base`.

## How `demo` differs from `base`

Same three modules (`network`, `state_stores`, `finops`), wired the same way,
with these differences:

- `environment = "demo"`. Every resource name and the common `Environment` tag
  carry `demo`, so `base` and `demo` resources never collide and cost can be
  filtered per environment.
- The FinOps guardrails apply unchanged. They are the soft budget, the hard cap
  with the IAM deny action, the Cost Explorer anomaly monitor, and the nightly
  teardown Lambda. A demo is when an idle, forgotten streaming job is most
  likely, so the teardown sweep matters more here.
- `teardown_dry_run = false` is the expected setting once trusted, so the
  nightly sweep actually stops lingering workloads.
- `enable_nat` stays `false`. A demo has no reason to pay for NAT.
- State stays in the local backend. A demo is short-lived, so its state does
  not need to move to S3.

## Shared conventions (identical to `base`)

- Variables: `project` (default `harbormaster`), `environment` (`demo` here),
  `aws_region` (default `us-east-1`), `platform_role_name`, `alert_email`.
- Common tags on every resource:
  `{ Project = var.project, Environment = var.environment, ManagedBy = "terraform" }`.
- Provider pins come from `infra/terraform/versions.tf`: `aws ~> 5.0`,
  `archive ~> 2.0`, `random ~> 3.0`, Terraform `>= 1.9`.

## When you build it out

Create `main.tf`, `variables.tf`, `outputs.tf`, `backend.tf`, and a
`terraform.tfvars.example` mirroring `envs/base`, changing only the `environment`
default to `demo` and the defaults noted above. The module `source` paths are the
same `../../modules/<name>` relative references.

## Stand up and tear down

```bash
# from infra/terraform/envs/demo, once main.tf exists
terraform init
terraform apply        # stand up the demo
# ... show the platform ...
terraform destroy      # tear everything down so nothing keeps running
```

The nightly teardown Lambda is the safety net if you forget to run
`terraform destroy`. It stops the expensive streaming and batch workloads. It
does not delete the cheap foundation (VPC, empty buckets, idle on-demand
tables), which costs almost nothing at rest.
