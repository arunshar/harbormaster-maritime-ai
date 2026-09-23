# envs/base/variables.tf
#
# Root-level variables for the Harbormaster "base" environment. Values come from
# terraform.tfvars (copy terraform.tfvars.example). Nothing here contains a real
# account id or secret.

variable "project" {
  description = "Project name, propagated to every module and the common tags."
  type        = string
  default     = "harbormaster"

  validation {
    condition     = var.project == "harbormaster"
    error_message = "project must be harbormaster because the IAM boundary and FinOps guardrails use that fixed namespace."
  }
}

variable "environment" {
  description = "Deployment environment for this root. Fixed to base here."
  type        = string
  default     = "base"

  validation {
    condition     = contains(["base", "demo"], var.environment)
    error_message = "environment must be one of: base, demo."
  }
}

variable "aws_region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-1"
}

variable "rds_allocated_storage_gb" {
  description = "Allocated storage for the base PostgreSQL instance. Increase only through a reviewed recovery plan because RDS cannot reduce allocated storage."
  type        = number
  default     = 20

  validation {
    condition     = var.rds_allocated_storage_gb >= 20
    error_message = "rds_allocated_storage_gb must be at least the original 20 GiB allocation."
  }
}

variable "rds_max_allocated_storage_gb" {
  description = "Optional RDS storage-autoscaling ceiling. Null leaves autoscaling disabled."
  type        = number
  default     = null

  validation {
    condition     = var.rds_max_allocated_storage_gb == null || var.rds_max_allocated_storage_gb >= var.rds_allocated_storage_gb
    error_message = "rds_max_allocated_storage_gb must be null or at least rds_allocated_storage_gb."
  }
}

variable "rds_iops" {
  description = "Optional gp3 IOPS setting. Null preserves the provider-managed default."
  type        = number
  default     = null
}

variable "rds_storage_throughput" {
  description = "Optional gp3 storage throughput in MiB/s. Null preserves the provider-managed default."
  type        = number
  default     = null
}

variable "alert_email" {
  description = "Email subscribed to budget and anomaly alerts. AWS sends a one-time confirmation link."
  type        = string
}

variable "platform_role_name" {
  description = <<-EOT
    Name of the IAM role the $75 hard-budget action attaches the deny policy to
    on breach. The permissions boundary grants this action only on the existing
    harbormaster-platform role.
  EOT
  type        = string

  validation {
    condition     = var.platform_role_name == "harbormaster-platform"
    error_message = "platform_role_name must be harbormaster-platform because the permissions boundary grants the budget action only on that role."
  }
}

variable "enable_nat" {
  description = "Create the single NAT gateway in the network module. Default false to avoid the hourly charge."
  type        = bool
  default     = false
}

variable "enable_nightly_teardown" {
  description = "Create the EventBridge schedule that runs the teardown Lambda nightly."
  type        = bool
  default     = true
}

variable "teardown_dry_run" {
  description = "When true, the teardown Lambda only logs what it would tear down. Keep true until you trust it."
  type        = bool
  default     = true
}

variable "cost_anomaly_monitor_arn" {
  description = <<-EOT
    Reuse an existing Cost Explorer SERVICE anomaly monitor instead of creating one.
    AWS allows only one dimensional SERVICE monitor per account and auto-creates a
    "Default-Services-Monitor", so set this to that monitor's ARN on such accounts.
    Empty creates our own monitor.
  EOT
  type        = string
  default     = ""
}

variable "enable_phase1" {
  description = <<-EOT
    Gate for the Phase 1 streaming + serving pipeline (kinesis, firehose, rds,
    and the compute plane). Default false keeps a base apply Phase-0-only and
    cheap. Set true for a demo apply, then back to false (or destroy) to stop
    the billable compute.
  EOT
  type        = bool
  default     = false
}

variable "serving_image" {
  description = <<-EOT
    Optional immutable serving image URI. Empty keeps the legacy :latest
    reference; live CDC validation should pass an ECR digest after review.
  EOT
  type        = string
  default     = ""
}

variable "enable_phase2" {
  description = <<-EOT
    Foundation gate for the Phase 2 CDC showcase plane: MSK Serverless, Redis
    on Fargate, the two CDC ECR repositories, and slot-lag monitoring. The
    application activation gate is enable_cdc_services. Default false keeps a
    base apply Phase-0-only. The AWS showcase also requires enable_phase1 =
    true (RDS, ECS cluster, serving); the local kind/Strimzi plane needs
    neither. Set true for a demo window, then back to false: MSK Serverless
    left running is the single biggest budget threat in the platform (~$18/day).
  EOT
  type        = bool
  default     = false

  validation {
    condition     = !var.enable_phase2 || var.enable_phase1
    error_message = "enable_phase2 requires enable_phase1 = true (RDS, the ECS cluster, and the Cloud Map namespace are Phase 1 resources)."
  }
}

variable "enable_cdc_services" {
  description = <<-EOT
    Second Phase 2 gate for the image-dependent CDC application path:
    Debezium Connect, the CDC consumer, and the serving service's Redis and
    online-table wiring. Default false keeps a foundation-only apply from
    creating application services or changing the live serving task
    definition. Set true only after both immutable image references exist in
    the Phase 2 ECR repositories, their scan results have been reviewed, and
    a separate service-activation plan has been approved. This does not
    register or alter a Debezium connector.
  EOT
  type        = bool
  default     = false

  validation {
    condition = !var.enable_cdc_services || (
      var.enable_phase2 &&
      trimspace(var.cdc_connect_image) != "" &&
      trimspace(var.cdc_consumer_image) != ""
    )
    error_message = "enable_cdc_services requires enable_phase2 = true plus non-empty cdc_connect_image and cdc_consumer_image values."
  }
}

variable "rds_logical_replication_enabled" {
  description = <<-EOT
    Keep PostgreSQL logical decoding enabled independently of the Phase 2 CDC
    service switch. True attaches the reviewed custom RDS parameter group with
    rds.logical_replication=1 and requires a separately planned reboot to take
    effect. It does not create MSK, Debezium, Redis, or CDC consumer resources.
  EOT
  type        = bool
  default     = false
}

variable "enable_phase3" {
  description = <<-EOT
    Gate for the Phase 3 lake + promotion plane (transient EMR backfill, the
    Feast offline store export, the cluster-to-S3 checkpoint manifest path, and the
    SageMaker async Pi-DPM endpoint + promotion pipeline). Default false keeps
    a base apply Phase-0-only. Requires enable_phase1 = true (the SageMaker
    endpoint sits in the Phase 1 VPC; the lake export reuses the Phase 0 lake
    bucket and Glue catalog). Does NOT require enable_phase2 (CDC is
    orthogonal to training/promotion). Set true for a demo window, then back
    to false: an EMR job left running and a SageMaker endpoint left above its
    zero-minimum auto-scaling capacity are this phase's budget threats.
  EOT
  type        = bool
  default     = false

  validation {
    condition     = !var.enable_phase3 || var.enable_phase1
    error_message = "enable_phase3 requires enable_phase1 = true (the VPC, lake bucket, and Glue catalog it depends on are Phase 0/1 resources)."
  }
}

variable "enable_phase4" {
  description = <<-EOT
    Gate for the Phase 4 drift-watch plane (modules/drift_watch: an
    EventBridge schedule -> Lambda running mlops/drift.py's input-drift check
    against two lake-bucket parquet snapshots -> the existing Phase 0 finops
    SNS topic). Default false; no resources exist behind this toggle yet
    (added at gate 4.6). Depends only on Phase 0 (the lake bucket and SNS
    topic), not Phase 1/3, since the module reads S3 snapshots rather than
    calling any Phase 1/3 service directly. Not applied during the 24-hour
    completion sprint (2026-07-04): authored, `terraform validate`- and
    plan-checksum-verified only, per docs/phases/PHASE_4.md gate 4.6.
  EOT
  type        = bool
  default     = false
}

variable "enable_phase5" {
  description = <<-EOT
    Gate for the Phase 5 multi-tenant EKS front-door plane (the EKS teardown
    guard at gate 5.0, then the EKS cluster + scale-to-zero spot node group +
    KEDA at gates 5.1/5.2). Default false keeps a base apply Phase-0-only and
    the plan a ZERO diff (every Phase 5 module is whole-module count-gated on
    this flag; nothing outside those modules changes on either value).
    Requires enable_phase1 = true (the EKS cluster sits in the Phase 1 VPC's
    private subnets), the enable_phase2/enable_phase3 convention. COST WATCH:
    the EKS control plane bills a flat ~$73/mo (~$0.10/hour) from cluster
    creation whether or not any node or pod runs, the one Phase 1-5 cost that
    cannot idle to zero, which is why this flag also arms the structural
    teardown guard (modules/eks_teardown_guard) that force-destroys the
    cluster after phase5_teardown_max_age_hours.
  EOT
  type        = bool
  default     = false

  validation {
    condition     = !var.enable_phase5 || var.enable_phase1
    error_message = "enable_phase5 requires enable_phase1 = true (the EKS cluster sits in the Phase 1 VPC's private subnets)."
  }
}

variable "phase5_teardown_max_age_hours" {
  description = <<-EOT
    Hours after EKS cluster creation before the gate 5.0 teardown guard
    force-destroys the node groups and cluster. Default 4 bounds a demo
    window to roughly $0.40 of control plane. Extend a LIVE demo by setting
    a future KeepAliveUntil tag on the cluster (gate 5.1 wires
    phase5_keep_alive_until to it), not by raising this window.
  EOT
  type        = number
  default     = 4

  validation {
    condition     = var.phase5_teardown_max_age_hours >= 0 && var.phase5_teardown_max_age_hours <= 8
    error_message = "phase5_teardown_max_age_hours must be between 0 for the scheduled proof and 8 for a bounded window."
  }
}

variable "phase5_keep_alive_until" {
  description = <<-EOT
    ISO 8601 UTC timestamp (e.g. "2026-07-12T02:00:00Z") stamped on the EKS
    cluster as its KeepAliveUntil tag, the gate 5.0 teardown guard's
    keep-alive contract: a future value extends a LIVE demo past
    phase5_teardown_max_age_hours; empty (the default) stamps nothing so the
    age window is the only clock. Unparseable values grant NO extension (the
    guard fails toward teardown). Extending a demo is a tfvars update + apply
    of this one tag, never a schedule or window rewrite.
  EOT
  type        = string
  default     = ""
}

variable "phase5_endpoint_public_access" {
  description = <<-EOT
    Temporarily expose the EKS API endpoint for a laptop-driven W4 window.
    Default false preserves the private-only posture. When true, the endpoint
    remains privately reachable too and phase5_public_access_cidrs must contain
    one or more operator host CIDRs.
  EOT
  type        = bool
  default     = false

  validation {
    condition     = !var.phase5_endpoint_public_access || var.enable_phase5
    error_message = "phase5_endpoint_public_access requires enable_phase5 = true."
  }

  validation {
    condition = (
      var.phase5_endpoint_public_access
      ? length(var.phase5_public_access_cidrs) > 0
      : length(var.phase5_public_access_cidrs) == 0
    )
    error_message = "phase5_endpoint_public_access requires at least one operator host CIDR when true and an empty list when false."
  }
}

variable "phase5_public_access_cidrs" {
  description = "Operator IPv4 host CIDRs for temporary W4 EKS API access. Only /32 entries are accepted."
  type        = list(string)
  default     = []

  validation {
    condition = alltrue([
      for cidr in var.phase5_public_access_cidrs :
      can(cidrhost(cidr, 0)) &&
      can(regex("^([0-9]{1,3}\\.){3}[0-9]{1,3}/32$", cidr))
    ])
    error_message = "phase5_public_access_cidrs entries must be valid IPv4 /32 host CIDRs, never world-open ranges."
  }
}

variable "phase5_node_desired_size" {
  description = "Terraform-managed EKS worker count. Set to 1 for W4 because KEDA scales serving pods, not nodes."
  type        = number
  default     = 0

  validation {
    condition     = var.phase5_node_desired_size >= 0 && var.phase5_node_desired_size <= 1
    error_message = "phase5_node_desired_size must be 0 or 1 for the bounded single-worker W4 design."
  }

  validation {
    condition     = var.phase5_node_desired_size == 0 || var.enable_nat
    error_message = "phase5_node_desired_size above 0 requires enable_nat = true until the private subnets have all required interface endpoints."
  }
}

variable "enable_phase5_kubernetes_access" {
  description = <<-EOT
    Allow Terraform's Helm provider to read live EKS credentials. This is
    separate from the KEDA release flag so cleanup can uninstall KEDA while
    the provider still reaches the cluster, then disable access before the
    teardown guard removes the API server.
  EOT
  type        = bool
  default     = false

  validation {
    condition     = !var.enable_phase5_kubernetes_access || var.enable_phase5
    error_message = "enable_phase5_kubernetes_access requires enable_phase5 = true."
  }
}

variable "enable_phase5_keda" {
  description = <<-EOT
    Gate for the KEDA helm_release AND the helm provider's cluster
    credentials. SEPARATE from enable_phase5 by necessity, not taste: a
    provider block cannot be count-gated, and the helm provider is configured
    from aws_eks_cluster/aws_eks_cluster_auth DATA sources that read live
    cluster credentials at plan time. Gating those data sources (and the
    helm_release) on this flag means every enable_phase5_keda = false plan,
    including make validate and CI, reads no live cluster credentials. The
    root's normal plan still performs its standing AWS account and AZ reads.
    The documented two-step: apply #1 with enable_phase5 = true creates the
    cluster; apply #2 adds enable_phase5_keda = true to install KEDA. Default
    false. Requires enable_phase5 (and an EXISTING cluster, which Terraform
    cannot cross-validate; the runbook owns that ordering).
  EOT
  type        = bool
  default     = false

  validation {
    condition     = !var.enable_phase5_keda || var.enable_phase5
    error_message = "enable_phase5_keda requires enable_phase5 = true (KEDA installs into the Phase 5 EKS cluster)."
  }

  validation {
    condition     = !var.enable_phase5_keda || var.enable_phase5_kubernetes_access
    error_message = "enable_phase5_keda requires enable_phase5_kubernetes_access = true so Terraform can reach the cluster."
  }

  validation {
    condition     = !var.enable_phase5_keda || var.phase5_node_desired_size >= 1
    error_message = "enable_phase5_keda requires phase5_node_desired_size >= 1 because no node autoscaler is installed."
  }

  validation {
    condition     = !var.enable_phase5_keda || var.enable_nat
    error_message = "enable_phase5_keda requires enable_nat = true until the private subnets have all required interface endpoints."
  }
}

variable "phase5_guard_dry_run" {
  description = <<-EOT
    When true, the teardown guard logs what it would destroy but takes no
    action. Default FALSE (armed), deliberately inverted from
    teardown_dry_run's safe-until-trusted convention: the guard being armed
    IS the structural mitigation, and a resting dry-run state would reduce it
    back to a procedural checklist. Set true only for a rehearsal window.
  EOT
  type        = bool
  default     = false
}

variable "phase5_teardown_schedule_expression" {
  description = "EventBridge Scheduler rate for the EKS guard. Keep rate(30 minutes) normally; W4 may temporarily use rate(5 minutes) for the scheduled force-delete proof."
  type        = string
  default     = "rate(30 minutes)"

  validation {
    condition = contains(
      ["rate(5 minutes)", "rate(30 minutes)"],
      var.phase5_teardown_schedule_expression,
    )
    error_message = "phase5_teardown_schedule_expression must be rate(5 minutes) for the proof or rate(30 minutes) normally."
  }
}

variable "serving_target" {
  description = "API Gateway serving backend: ecs (default and rollback) or eks (W4 only, requires enable_phase5)."
  type        = string
  default     = "ecs"

  validation {
    condition     = contains(["ecs", "eks"], var.serving_target)
    error_message = "serving_target must be one of: ecs, eks."
  }

  validation {
    condition     = var.serving_target != "eks" || var.enable_phase5
    error_message = "serving_target = eks requires enable_phase5 = true."
  }
}

variable "enable_cmk" {
  description = <<-EOT
    Gate for the customer-managed KMS key (modules/kms: rotation-enabled key +
    alias/harbormaster-<environment>, wired into the S3 buckets, DynamoDB
    tables, RDS storage, and every CloudWatch log group). Default false keeps
    the platform on its original encryption (S3 SSE-AES256, AWS-managed keys
    elsewhere) and the plan a ZERO diff; every consumer's kms_key_arn input
    collapses to its pre-CMK configuration when empty. Costs roughly $1 per
    key per month plus usage when enabled. NOTE: flipping this on with an
    EXISTING RDS instance forces its replacement (kms_key_id is a create-time
    attribute); acceptable because Phase 1 is torn down between demo windows.
  EOT
  type        = bool
  default     = false
}

variable "pidpm_image" {
  description = <<-EOT
    ECR image URI wrapping the frozen PiDpmScorer contract (built from
    mlops/pidpm_container/Dockerfile and pushed at demo time). Empty skips
    the SageMaker model/endpoint, the flink_code_s3_key pattern, so an apply
    before the image push and the checkpoint export creates no
    half-configured endpoint.
  EOT
  type        = string
  default     = ""
}

variable "pidpm_model_data_url" {
  description = <<-EOT
    S3 URI to the exported Pi-DPM checkpoint artifact (from
    mlops/manifest.py's one-way export). Empty skips the SageMaker
    model/endpoint, same reasoning as pidpm_image.
  EOT
  type        = string
  default     = ""
}

variable "enable_pidpm_failure_alerts" {
  description = <<-EOT
    Enable the opt-in Pi-DPM failure-alert topology: model-bucket EventBridge
    events filtered to pidpm/async-failure/, a dedicated SNS topic and
    configured alert_email subscription, and the scoped SageMaker
    InvocationFailures CloudWatch alarm. Disabled by default. It may be enabled
    only with a configured Phase 3 endpoint, and remains pending until a
    reviewed plan, email confirmation, malformed-payload drill, alarm-state
    observation, and operator acknowledgement provide external evidence.
  EOT
  type        = bool
  default     = false

  validation {
    condition = !var.enable_pidpm_failure_alerts || (
      var.enable_phase3 && var.pidpm_image != "" && var.pidpm_model_data_url != ""
    )
    error_message = "enable_pidpm_failure_alerts requires enable_phase3 plus non-empty pidpm_image and pidpm_model_data_url values."
  }
}

variable "pidpm_candidate_model_data_url" {
  description = <<-EOT
    Reserved compatibility input for a challenger Pi-DPM checkpoint. This
    deployment is a SageMaker Async Inference endpoint, which supports one
    production variant, so non-empty input is rejected at Terraform plan
    time. Candidate comparison requires a separately reviewed endpoint.
  EOT
  type        = string
  default     = ""
}

variable "cdc_connect_image" {
  description = <<-EOT
    ECR image URI for Debezium Connect (built from cdc/connect/Dockerfile and
    pushed at demo time). The service requires both this non-empty value and
    enable_cdc_services = true, so a foundation apply before the image push
    creates no crash-looping service.
  EOT
  type        = string
  default     = ""
}

variable "cdc_consumer_image" {
  description = <<-EOT
    ECR image URI for the CDC consumer (built from cdc/consumer/Dockerfile and
    pushed at demo time). The service requires both this non-empty value and
    enable_cdc_services = true.
  EOT
  type        = string
  default     = ""
}

variable "slot_lag_alarm_bytes" {
  description = <<-EOT
    Replication-slot lag threshold in bytes for the Phase 2 CloudWatch alarm.
    The production default is 200 MB. A lower value is allowed only for a
    bounded acceptance drill and must be restored immediately afterward.
  EOT
  type        = number
  default     = 209715200

  validation {
    condition     = var.slot_lag_alarm_bytes > 0
    error_message = "slot_lag_alarm_bytes must be positive."
  }
}

variable "flink_code_s3_key" {
  description = <<-EOT
    S3 key (in the lake bucket) of the packaged Flink job zip (make
    flink-package, uploaded to s3://<lake_bucket>/flink/flink-app.zip at
    demo time). Empty skips creating the Managed Flink application (gate
    1.5's modules/kda_flink gates create_app on this being non-empty), so a
    Phase 1 apply before the artifact is uploaded creates no
    half-configured Flink app. Despite three comments elsewhere in this
    repo referring to "the flink_code_s3_key pattern" as an existing
    env-level toggle (mirroring pidpm_image/cdc_connect_image), this
    variable and its wiring into module.kda_flink never actually existed
    until the first live Phase 1 W1 sprint-window run found the gap.
  EOT
  type        = string
  default     = ""
}

variable "permissions_boundary_name" {
  description = <<-EOT
    Name of the IAM permissions-boundary policy (created by
    infra/aws/bootstrap.sh as harbormaster-permissions-boundary) attached to
    every module-created role. envs/base derives the ARN from this and the
    caller account id and passes it into every role-creating module, so the
    boundary-gated harbormaster-platform deploy policy can create them (war
    story P32, the two-sided contract). Roles are count-gated on enable_phaseN,
    so the default phases-off plan creates none. Set to "" to disable the
    boundary (not recommended once the boundary-gated deploy policy is live).
  EOT
  type        = string
  default     = "harbormaster-permissions-boundary"
}
