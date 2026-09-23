# modules/emr_backfill
#
# The transient MarineCadastre backfill (Phase 3, gate 3.2): an EMR Serverless
# Spark application that writes ais_history + corridor_graph_nodes/edges to
# the lake bucket's Iceberg tables via the Glue catalog. Auto-terminate is
# structural, not procedural: auto_stop_configuration idles the application
# down after idle_timeout_minutes with no standing cluster to forget about,
# and job runs themselves are submitted (operator-run, `aws emr-serverless
# start-job-run`) rather than created as a long-lived Terraform resource,
# matching the CDC connector's "scripted registration, not a TF resource"
# pattern. Whole-module gate at the envs/base call site (count =
# var.enable_phase3 ? 1 : 0), matching ecs_cdc_consumer: this is a standalone
# always-off-until-invoked resource, not an internal-toggle case like
# rds.logical_replication.

variable "project" {
  type    = string
  default = "harbormaster"
}

variable "environment" {
  type = string
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "release_label" {
  description = "EMR Serverless Spark release. Kept as a variable so it can be bumped without touching the resource block."
  type        = string
  default     = "emr-7.2.0"
}

variable "idle_timeout_minutes" {
  description = "Auto-stop the application after this many idle minutes. This IS the auto-terminate mechanism (structural, not a checklist item)."
  type        = number
  default     = 15
}

variable "raw_extract_s3_uri" {
  description = "S3 prefix holding the raw MarineCadastre extract the backfill job reads."
  type        = string
}

variable "lake_bucket_arn" {
  type = string
}

variable "glue_database" {
  description = "Glue database (Iceberg namespace) the backfill writes into. Matches lake/iceberg.py's default namespace."
  type        = string
  default     = "hm"
}

variable "log_retention_days" {
  type    = number
  default = 14
}

variable "kms_key_arn" {
  description = "ARN of the customer-managed KMS key for log-group encryption. Empty (the default) keeps the CloudWatch Logs default encryption, so the default plan stays a zero diff."
  type        = string
  default     = ""
}

variable "tags" {
  type    = map(string)
  default = {}
}

variable "permissions_boundary_arn" {
  description = "ARN of the IAM permissions boundary to attach to roles this module creates. Empty attaches no boundary. The harbormaster-platform deploy policy requires the harbormaster-permissions-boundary on every managed role (see war story P32, the two-sided contract), so envs/base sets this at apply time."
  type        = string
  default     = ""
}

locals {
  name_prefix = "${var.project}-${var.environment}"
  tags        = merge(var.tags, { Module = "emr_backfill" })
}

data "aws_caller_identity" "current" {}

resource "aws_cloudwatch_log_group" "backfill" {
  name              = "/harbormaster/${var.environment}/lake-backfill"
  retention_in_days = var.log_retention_days
  # CMK when set; null keeps the CloudWatch Logs default encryption (zero diff).
  kms_key_id = var.kms_key_arn != "" ? var.kms_key_arn : null
  tags       = local.tags
}

resource "aws_emrserverless_application" "backfill" {
  name          = "${local.name_prefix}-lake-backfill"
  release_label = var.release_label
  type          = "SPARK"

  auto_start_configuration {
    enabled = true
  }

  # Structural auto-terminate: no job means no standing compute, and any job
  # left completed-but-idle stops billing after idle_timeout_minutes without
  # a human remembering to tear anything down.
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = var.idle_timeout_minutes
  }

  # No monitoring_configuration block on the application itself (EMR
  # Serverless configures logging per job run, not on the application
  # resource); job submissions pass
  # --job-driver monitoringConfiguration.cloudWatchLoggingConfiguration
  # pointed at aws_cloudwatch_log_group.backfill.name below.

  tags = merge(local.tags, { Name = "${local.name_prefix}-lake-backfill" })
}

data "aws_iam_policy_document" "job_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "job_execution" {
  name                 = "${local.name_prefix}-lake-backfill-job"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null
  assume_role_policy   = data.aws_iam_policy_document.job_assume.json
  tags                 = local.tags
}

data "aws_iam_policy_document" "job_execution" {
  statement {
    sid    = "ReadRawExtract"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      var.raw_extract_s3_uri,
      "${var.raw_extract_s3_uri}/*",
    ]
  }

  statement {
    sid    = "IcebergLakeReadWrite"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      var.lake_bucket_arn,
      "${var.lake_bucket_arn}/iceberg/*",
    ]
  }

  statement {
    # The job's own entry point script, --py-files zip, and venv archive are
    # uploaded to <lake_bucket>/code/ (scripts/package_lake_for_emr.sh --upload,
    # the runbook's Part 1.3), a path this role never had read access to: the
    # first live EMR run failed at driver startup with
    # "FileNotFoundException: File s3://.../code/lake_backfill_job.py does not
    # exist" even though the object existed, because IAM silently denied the
    # GetObject and Spark surfaces that as FileNotFoundException, not
    # AccessDenied (a real first-live-run finding, W2 sprint window,
    # 2026-07-04).
    sid    = "ReadJobCode"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      var.lake_bucket_arn,
      "${var.lake_bucket_arn}/code/*",
    ]
  }

  statement {
    sid    = "IcebergGlueCatalog"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:CreateDatabase",
      "glue:GetTable",
      "glue:CreateTable",
      "glue:UpdateTable",
    ]
    resources = [
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${var.glue_database}",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${var.glue_database}/*",
    ]
  }

  statement {
    sid    = "BackfillLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["${aws_cloudwatch_log_group.backfill.arn}:*"]
  }
}

resource "aws_iam_role_policy" "job_execution" {
  name   = "${local.name_prefix}-lake-backfill-job"
  role   = aws_iam_role.job_execution.id
  policy = data.aws_iam_policy_document.job_execution.json
}

output "application_id" {
  value = aws_emrserverless_application.backfill.id
}

output "execution_role_arn" {
  value = aws_iam_role.job_execution.arn
}
