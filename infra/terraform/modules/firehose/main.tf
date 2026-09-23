# modules/firehose/main.tf
#
# Kinesis Data Firehose that tees the ais-raw stream into the S3 data lake under
# raw/, date-partitioned. This is the raw landing zone; Parquet/Iceberg format
# conversion and Glue-table registration are layered in the lake phase (Phase 3).
# Buffering is intentionally small (60s / 5 MB) so demo data lands promptly.

variable "project" {
  type    = string
  default = "harbormaster"
}

variable "environment" {
  type = string
}

variable "kinesis_stream_arn" {
  type = string
}

variable "lake_bucket_arn" {
  type = string
}

variable "buffering_interval_seconds" {
  type    = number
  default = 60
}

variable "buffering_size_mb" {
  type    = number
  default = 5
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
  tags        = merge(var.tags, { Module = "firehose" })
}

# ---- IAM: let Firehose read the stream and write the lake ----

data "aws_iam_policy_document" "assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "firehose" {
  name                 = "${local.name_prefix}-firehose-ais-raw"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null
  assume_role_policy   = data.aws_iam_policy_document.assume.json
  tags                 = local.tags
}

data "aws_iam_policy_document" "firehose" {
  statement {
    sid    = "ReadStream"
    effect = "Allow"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:GetShardIterator",
      "kinesis:GetRecords",
      "kinesis:ListShards",
    ]
    resources = [var.kinesis_stream_arn]
  }

  statement {
    sid    = "WriteLake"
    effect = "Allow"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject",
    ]
    resources = [
      var.lake_bucket_arn,
      "${var.lake_bucket_arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "firehose" {
  name   = "${local.name_prefix}-firehose-ais-raw"
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.firehose.json
}

# ---- Delivery stream: Kinesis source -> S3 lake (raw/) ----

resource "aws_kinesis_firehose_delivery_stream" "ais_raw" {
  name        = "${local.name_prefix}-ais-raw-to-lake"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = var.kinesis_stream_arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose.arn
    bucket_arn          = var.lake_bucket_arn
    prefix              = "raw/ingest_date=!{timestamp:yyyy-MM-dd}/"
    error_output_prefix = "raw_errors/!{firehose:error-output-type}/ingest_date=!{timestamp:yyyy-MM-dd}/"
    buffering_interval  = var.buffering_interval_seconds
    buffering_size      = var.buffering_size_mb
    compression_format  = "GZIP"
  }

  tags = local.tags
}

output "delivery_stream_name" {
  value = aws_kinesis_firehose_delivery_stream.ais_raw.name
}

output "delivery_stream_arn" {
  value = aws_kinesis_firehose_delivery_stream.ais_raw.arn
}

output "firehose_role_arn" {
  value = aws_iam_role.firehose.arn
}
