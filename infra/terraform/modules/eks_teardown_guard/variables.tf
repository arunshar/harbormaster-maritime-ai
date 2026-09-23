# modules/eks_teardown_guard/variables.tf

variable "project" {
  description = "Project name, used in tags and resource names."
  type        = string
  default     = "harbormaster"
}

variable "environment" {
  description = "Deployment environment: base or demo."
  type        = string

  validation {
    condition     = contains(["base", "demo"], var.environment)
    error_message = "environment must be one of: base, demo."
  }
}

variable "aws_region" {
  description = "AWS region, used to build the scoped EKS/logs ARNs in the guard's IAM policy and KMS key policy."
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = <<-EOT
    Name of the EKS cluster the guard force-destroys. Deliberately a plain
    string, never a module output: the guard must have zero Terraform
    dependency on the cluster it guards so it can be applied before the
    cluster exists and survives the cluster's destruction. Empty (the
    default) derives "<project>-<environment>-eks", the name
    modules/eks_cluster builds from the same inputs.
  EOT
  type        = string
  default     = ""
}

variable "max_age_hours" {
  description = <<-EOT
    Hours after cluster creation before the guard force-destroys it (the
    gate 5.0 default of 4 bounds a demo window to roughly $0.40 of control
    plane). Extending a live demo is a KeepAliveUntil tag update on the
    cluster, not a change to this window.
  EOT
  type        = number
  default     = 4

  validation {
    condition     = var.max_age_hours >= 0 && var.max_age_hours <= 8
    error_message = "max_age_hours must be between 0 for the scheduled proof and 8 for a bounded window."
  }
}

variable "keep_alive_tag_key" {
  description = "Cluster tag key holding the ISO 8601 keep-alive timestamp. Unparseable values grant NO extension (the guard fails toward teardown)."
  type        = string
  default     = "KeepAliveUntil"
}

variable "schedule_expression" {
  description = <<-EOT
    How often the guard re-evaluates the cluster's age (UTC). A recurring
    rate, not a one-shot at(): a missed tick self-heals on the next one, and
    the node-groups-first delete converges across runs. 30 minutes bounds
    the worst-case overshoot past max_age_hours to ~$0.05 of control plane.
  EOT
  type        = string
  default     = "rate(30 minutes)"

  validation {
    condition     = contains(["rate(5 minutes)", "rate(30 minutes)"], var.schedule_expression)
    error_message = "schedule_expression must be rate(5 minutes) for the proof or rate(30 minutes) normally."
  }
}

variable "guard_dry_run" {
  description = <<-EOT
    When true, the guard logs what it would destroy but takes no action.
    Default FALSE, deliberately inverted from the finops teardown_dry_run
    convention: an armed guard is this module's entire purpose (the
    structural mitigation gate 5.0 promised), and a resting dry-run state
    would quietly reduce it back to a procedural checklist. Set true only
    for a rehearsal window.
  EOT
  type        = bool
  default     = false
}

variable "sns_topic_arn" {
  description = "Existing Phase 0 finops SNS topic (module.finops.sns_topic_arn); receives the guard's action summary and serves as the Lambda dead-letter target. No new topic is created here."
  type        = string
}

variable "lambda_source_dir" {
  description = "Path to the guard Lambda source directory (infra/lambda/eks_teardown), zipped by archive_file at plan time, the modules/finops teardown packaging convention."
  type        = string
}

variable "lambda_runtime" {
  description = "Python runtime for the guard Lambda."
  type        = string
  default     = "python3.12"
}

variable "lambda_timeout_seconds" {
  description = "Guard Lambda timeout in seconds. Node-group and cluster deletes are async fire-and-forget calls, so the default is generous, not load-bearing."
  type        = number
  default     = 120
}

variable "log_retention_days" {
  description = "Retention for the guard's log group. 365 so the teardown audit trail outlives any demo season."
  type        = number
  default     = 365
}

variable "permissions_boundary_arn" {
  description = "ARN of the IAM permissions boundary to attach to roles this module creates. Empty attaches no boundary. The harbormaster-platform deploy policy requires the harbormaster-permissions-boundary on every managed role (see war story P32, the two-sided contract), so envs/base sets this at apply time."
  type        = string
  default     = ""
}

variable "tags" {
  description = "Common tags applied to every resource."
  type        = map(string)
  default     = {}
}
