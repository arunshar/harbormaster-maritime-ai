# modules/drift_watch
#
# Phase 4 gate 4.6: the drift-alerting plane. An EventBridge Scheduler
# schedule invokes a Lambda (infra/lambda/drift_watch/handler.py) that reads
# two parquet snapshots from the Phase 0 lake bucket, calls the gate 4.1
# check_input_drift port, and publishes an alert to the EXISTING Phase 0
# finops SNS topic on any drifted feature (no new topic, matching this
# repo's reuse-anchor discipline). No VPC: unlike modules/cdc_monitoring
# (which reaches RDS in a private subnet), this Lambda only ever calls S3
# and SNS, both reachable over the public AWS API surface with no VPC
# attachment, no ENI, no VPC endpoints, and no associated cost.
#
# Whole-module gate at the envs/base call site (count = var.enable_phase4 ?
# 1 : 0), matching modules/emr_backfill's convention. NOT applied during the
# 24-hour completion sprint (2026-07-04): authored, `terraform validate`- and
# plan-checksum-verified only, per docs/phases/PHASE_4.md gate 4.6.

variable "project" {
  type    = string
  default = "harbormaster"
}

variable "environment" {
  type = string
}

variable "lake_bucket_arn" {
  type = string
}

variable "lake_bucket_name" {
  type = string
}

variable "lambda_artifacts_bucket_name" {
  description = "Existing Phase 0 models bucket used to stage the Lambda ZIP when its compressed size exceeds Lambda's direct-upload limit."
  type        = string
}

variable "reference_snapshot_key" {
  description = "S3 key of the reference (most recent accepted gate 3.3 training-set export) parquet snapshot."
  type        = string
  default     = "drift/reference.parquet"
}

variable "current_snapshot_key" {
  description = "S3 key of the current-window parquet snapshot the scheduled job refreshes."
  type        = string
  default     = "drift/current.parquet"
}

variable "snapshot_source_prefix" {
  description = "Identified raw AIS S3 prefix. The Lambda lists Parquet candidates here, newest first, and rejects invalid data before publication."
  type        = string
  default     = "raw/marinecadastre/"

  validation {
    condition     = endswith(var.snapshot_source_prefix, "/")
    error_message = "snapshot_source_prefix must end in '/'."
  }
}

variable "snapshot_window_prefix" {
  description = "Content-addressed immutable validated-window and provenance prefix. The mutable current pointer is advanced only after both objects exist."
  type        = string
  default     = "drift/windows/"

  validation {
    condition     = endswith(var.snapshot_window_prefix, "/")
    error_message = "snapshot_window_prefix must end in '/'."
  }
}

variable "snapshot_min_rows" {
  description = "Minimum rows required for a source candidate. One supports the small recorded demonstration data only; raise it before a production-scale source is used."
  type        = number
  default     = 1

  validation {
    condition     = var.snapshot_min_rows >= 1
    error_message = "snapshot_min_rows must be at least one."
  }
}

variable "snapshot_max_rows" {
  description = "Maximum time-ordered AIS rows retained in a current drift window."
  type        = number
  default     = 10000

  validation {
    condition     = var.snapshot_max_rows >= var.snapshot_min_rows
    error_message = "snapshot_max_rows must be at least snapshot_min_rows."
  }
}

variable "sns_topic_arn" {
  description = "The existing Phase 0 finops SNS topic (module.finops.sns_topic_arn); no new topic is created here."
  type        = string
}

variable "schedule_expression" {
  description = "How often the drift check runs. Daily is enough for input drift (a slow-moving population signal), unlike Phase 2's 1-minute replication-lag check."
  type        = string
  default     = "rate(1 day)"
}

variable "lambda_source_dir" {
  description = "The packaged build dir (see make drift-lambda-package)."
  type        = string
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
  tags        = merge(var.tags, { Module = "drift_watch" })
}

data "archive_file" "drift_watch" {
  type        = "zip"
  source_dir  = var.lambda_source_dir
  output_path = "${path.module}/.build/drift-watch.zip"
}

resource "aws_s3_object" "drift_watch_package" {
  bucket = var.lambda_artifacts_bucket_name
  key    = "lambda/drift-watch/${data.archive_file.drift_watch.output_md5}.zip"
  source = data.archive_file.drift_watch.output_path
  # Large ZIP uploads use multipart S3 ETags, which do not equal the local MD5.
  # source_hash tracks the local archive content without asserting an ETag shape.
  source_hash = data.archive_file.drift_watch.output_base64sha256
  tags        = local.tags
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "drift_watch" {
  name                 = "${local.name_prefix}-drift-watch"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null
  assume_role_policy   = data.aws_iam_policy_document.lambda_assume.json
  tags                 = local.tags
}

resource "aws_iam_role_policy_attachment" "basic_logs" {
  role       = aws_iam_role.drift_watch.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "drift_watch" {
  statement {
    sid    = "ReadReferenceCurrentRawAndValidatedWindows"
    effect = "Allow"
    actions = [
      "s3:GetObject",
    ]
    resources = [
      "${var.lake_bucket_arn}/${var.reference_snapshot_key}",
      "${var.lake_bucket_arn}/${var.current_snapshot_key}",
      "${var.lake_bucket_arn}/${var.snapshot_source_prefix}*",
      "${var.lake_bucket_arn}/${var.snapshot_window_prefix}*",
    ]
  }

  statement {
    # HeadObject returns an indistinguishable 403 for a missing object when
    # ListBucket is absent. The immutable-window preflight therefore needs
    # this same prefix-scoped list permission, but never list access elsewhere.
    sid       = "ListRawAisCandidatesAndValidatedWindows"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [var.lake_bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        "${var.snapshot_source_prefix}*",
        "${var.snapshot_window_prefix}*",
      ]
    }
  }

  # Deliberately excludes reference_snapshot_key and every raw object. The
  # Lambda may only publish content-addressed validated windows and replace
  # the current pointer after both immutable artifacts are present.
  statement {
    sid    = "PublishValidatedWindowAndAdvanceCurrentOnly"
    effect = "Allow"
    actions = [
      "s3:PutObject",
    ]
    resources = [
      "${var.lake_bucket_arn}/${var.current_snapshot_key}",
      "${var.lake_bucket_arn}/${var.snapshot_window_prefix}*",
    ]
  }

  statement {
    sid       = "PublishDriftAlert"
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = [var.sns_topic_arn]
  }
}

resource "aws_iam_role_policy" "drift_watch" {
  name   = "${local.name_prefix}-drift-watch"
  role   = aws_iam_role.drift_watch.id
  policy = data.aws_iam_policy_document.drift_watch.json
}

resource "aws_lambda_function" "drift_watch" {
  function_name    = "${local.name_prefix}-drift-watch"
  role             = aws_iam_role.drift_watch.arn
  runtime          = "python3.12"
  handler          = "handler.handler"
  s3_bucket        = var.lambda_artifacts_bucket_name
  s3_key           = aws_s3_object.drift_watch_package.key
  source_code_hash = data.archive_file.drift_watch.output_base64sha256
  timeout          = 60
  memory_size      = 512        # pandas/pyarrow parquet parsing needs more headroom than the slot-lag Lambda's 128MB
  architectures    = ["x86_64"] # matches Makefile's manylinux2014_x86_64 wheel target

  environment {
    variables = {
      LAKE_BUCKET            = var.lake_bucket_name
      REFERENCE_SNAPSHOT_KEY = var.reference_snapshot_key
      CURRENT_SNAPSHOT_KEY   = var.current_snapshot_key
      SNAPSHOT_SOURCE_PREFIX = var.snapshot_source_prefix
      SNAPSHOT_WINDOW_PREFIX = var.snapshot_window_prefix
      SNAPSHOT_MIN_ROWS      = tostring(var.snapshot_min_rows)
      SNAPSHOT_MAX_ROWS      = tostring(var.snapshot_max_rows)
      SNS_TOPIC_ARN          = var.sns_topic_arn
    }
  }

  tags = local.tags
}

resource "aws_cloudwatch_log_group" "drift_watch" {
  name              = "/aws/lambda/${aws_lambda_function.drift_watch.function_name}"
  retention_in_days = var.log_retention_days
  # CMK when set; null keeps the CloudWatch Logs default encryption (zero diff).
  kms_key_id = var.kms_key_arn != "" ? var.kms_key_arn : null
  tags       = local.tags
}

data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name                 = "${local.name_prefix}-drift-watch-scheduler"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null
  assume_role_policy   = data.aws_iam_policy_document.scheduler_assume.json
  tags                 = local.tags
}

data "aws_iam_policy_document" "scheduler_invoke" {
  statement {
    effect    = "Allow"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.drift_watch.arn]
  }
}

resource "aws_iam_role_policy" "scheduler_invoke" {
  name   = "${local.name_prefix}-drift-watch-scheduler-invoke"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler_invoke.json
}

resource "aws_scheduler_schedule" "drift_watch" {
  name = "${local.name_prefix}-drift-watch"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_lambda_function.drift_watch.arn
    role_arn = aws_iam_role.scheduler.arn
  }
}

output "function_name" {
  value = aws_lambda_function.drift_watch.function_name
}

output "function_arn" {
  value = aws_lambda_function.drift_watch.arn
}
