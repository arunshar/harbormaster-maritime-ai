# modules/kda_flink/main.tf
#
# Amazon Managed Service for Apache Flink (KDA v2): computes AIS features and
# calls the scorer. The IAM role and log group are always created (free). The
# Flink application itself is gated behind flink_code_s3_key: it is created only
# once the 1.5 build uploads the job artifact to S3, so a 1.3 demo apply stands
# up the plumbing without incurring KPU cost. Flink calls the public API Gateway
# endpoint, so it needs no VPC configuration.

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

variable "kinesis_stream_arn" {
  type = string
}

variable "kinesis_stream_name" {
  description = "Plain stream name (not ARN); FlinkKinesisConsumer's constructor takes a name, and job.py reads it from Runtime Properties, not the ARN."
  type        = string
}

variable "feast_table_name" {
  type = string
}

variable "serving_endpoint" {
  description = "The serving API's invoke URL (API Gateway) job.py POSTs scored events to. Empty until the Phase 1 apply that creates module.apigw completes."
  type        = string
  default     = ""
}

variable "serving_api_execution_arn" {
  description = "API Gateway execution ARN for the one SigV4-authorized serving API."
  type        = string
}

variable "lake_bucket_arn" {
  type = string
}

variable "quarantine_bucket" {
  description = "Plain S3 bucket name (not ARN) for the streaming dead-letter/quarantine sink: malformed AIS and unrecoverable scorer POSTs land under quarantine/. Reuses the lake bucket (the Flink role already has s3:PutObject on it via LakeReadWrite). Empty disables the S3 DLQ; the job still logs+counts drops."
  type        = string
  default     = ""
}

variable "code_bucket_arn" {
  type    = string
  default = ""
}

variable "flink_code_s3_key" {
  type    = string
  default = ""
}

variable "runtime_environment" {
  type    = string
  default = "FLINK-1_20"
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
  tags        = merge(var.tags, { Module = "kda_flink" })
  create_app  = var.flink_code_s3_key != ""
}

data "aws_iam_policy_document" "assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["kinesisanalytics.amazonaws.com"]
    }
  }

}

resource "aws_iam_role" "flink" {
  name                 = "${local.name_prefix}-flink"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null
  assume_role_policy   = data.aws_iam_policy_document.assume.json
  tags                 = local.tags
}

data "aws_iam_policy_document" "flink" {
  statement {
    sid    = "ReadStream"
    effect = "Allow"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetShardIterator",
      "kinesis:GetRecords",
      "kinesis:ListShards",
    ]
    resources = [var.kinesis_stream_arn]
  }

  statement {
    sid    = "WriteFeatures"
    effect = "Allow"
    actions = [
      "dynamodb:PutItem",
      "dynamodb:BatchWriteItem",
      "dynamodb:UpdateItem",
    ]
    resources = ["arn:aws:dynamodb:${var.aws_region}:*:table/${var.feast_table_name}"]
  }

  statement {
    sid    = "LakeReadWrite"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
      "s3:PutObject",
    ]
    resources = [
      var.lake_bucket_arn,
      "${var.lake_bucket_arn}/*",
    ]
  }

  statement {
    sid       = "Logs"
    effect    = "Allow"
    actions   = ["logs:PutLogEvents", "logs:DescribeLogGroups", "logs:DescribeLogStreams"]
    resources = ["arn:aws:logs:${var.aws_region}:*:*"]
  }

  statement {
    sid       = "InvokeServingApi"
    effect    = "Allow"
    actions   = ["execute-api:Invoke"]
    resources = ["${var.serving_api_execution_arn}/$default/POST/v1/score-ais"]
  }
}

resource "aws_iam_role_policy" "flink" {
  name   = "${local.name_prefix}-flink"
  role   = aws_iam_role.flink.id
  policy = data.aws_iam_policy_document.flink.json
}

resource "aws_cloudwatch_log_group" "flink" {
  name              = "/harbormaster/${var.environment}/flink"
  retention_in_days = var.log_retention_days
  # CMK when set; null keeps the CloudWatch Logs default encryption (zero diff).
  kms_key_id = var.kms_key_arn != "" ? var.kms_key_arn : null
  tags       = local.tags
}

resource "aws_cloudwatch_log_stream" "flink" {
  name           = "flink-app"
  log_group_name = aws_cloudwatch_log_group.flink.name
}

# Flink application: created only once the 1.5 artifact exists (flink_code_s3_key).
resource "aws_kinesisanalyticsv2_application" "flink" {
  count = local.create_app ? 1 : 0

  name                   = "${local.name_prefix}-flink"
  runtime_environment    = var.runtime_environment
  service_execution_role = aws_iam_role.flink.arn

  application_configuration {
    application_code_configuration {
      code_content {
        s3_content_location {
          bucket_arn = var.code_bucket_arn
          file_key   = var.flink_code_s3_key
        }
      }
      code_content_type = "ZIPFILE"
    }

    flink_application_configuration {
      parallelism_configuration {
        configuration_type = "DEFAULT"
      }
    }

    # Runtime Properties: NOT plain OS env vars (confirmed against AWS's own
    # PyFlink example, docs/phases/PHASE_1.md's real-run finding). AWS writes
    # these to /etc/flink/application_properties.json at container start;
    # job.py reads them via PropertyGroupId lookup, never os.environ.
    environment_properties {
      # Mandatory for a PyFlink (non-Studio) application: tells Managed Flink
      # which script is the entry point and where the fat-jar (Kinesis
      # connector) lives inside the zip. Without these two keys the app
      # fails to start even after CreateApplication succeeds.
      property_group {
        property_group_id = "kinesis.analytics.flink.run.options"
        property_map = {
          python  = "main.py"
          jarfile = "lib/pyflink-dependencies.jar"
          # No pyFiles: the application zip carries flink/window_logic.py for the
          # driver import, then job.py registers that module for cloudpickle
          # by-value serialization into the UDF worker. The worker therefore does
          # not need a separately staged Python dependency.
          # Three attempts to ship those packages as a runtime dependency (env.
          # add_python_file, pyFiles as two comma-separated paths, pyFiles as one merged
          # directory) each hit a real, confirmed bug in Managed Flink's Python
          # dependency staging; real first-live-run findings, W1 sprint window,
          # 2026-07-04.
        }
      }

      property_group {
        property_group_id = "FlinkJob"
        # Kinesis Analytics v2 rejects any property_map value with zero length
        # (ValidationException: "Member must have length greater than or equal
        # to 1"), so quarantine_bucket, whose whole "disabled" contract is an
        # empty string (see the variable doc), must be OMITTED from the map
        # entirely when empty, not passed through as "". merge() with a
        # conditional empty map keeps the key out unless a real bucket name is
        # set; job.py's FeatureProcess._quarantine already treats a MISSING key
        # the same as an empty one (getProperty returns null either way), so
        # this changes nothing about the job's runtime behavior, only what
        # reaches the AWS API.
        property_map = merge(
          {
            kinesis_stream_name = var.kinesis_stream_name
            feast_online_table  = var.feast_table_name
            serving_endpoint    = var.serving_endpoint
            aws_region          = var.aws_region
          },
          var.quarantine_bucket != "" ? { quarantine_bucket = var.quarantine_bucket } : {}
        )
      }
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = aws_cloudwatch_log_stream.flink.arn
  }

  tags = local.tags
}

output "role_arn" {
  value = aws_iam_role.flink.arn
}

output "log_group_name" {
  value = aws_cloudwatch_log_group.flink.name
}

output "application_name" {
  value = one(aws_kinesisanalyticsv2_application.flink[*].name)
}
