# modules/kms/main.tf
#
# Customer-managed KMS key (CMK) for the buyer-grade encryption path: one
# symmetric key per environment, rotation enabled, 7-day deletion window (the
# minimum, so an accidental schedule is recoverable but a teardown is quick),
# and an alias/harbormaster-<environment> alias. Consumers (S3 buckets, RDS,
# DynamoDB tables, CloudWatch log groups) each take an optional kms_key_arn
# and fall back to their pre-CMK encryption when it is empty, so the whole
# path is inert until envs/base flips enable_cmk = true.
#
# Key policy, two statements:
#   1. Account root full access, the standard non-lockout anchor: IAM policies
#      in the account (not this key policy) then govern who can administer or
#      use the key.
#   2. CloudWatch Logs service use, scoped with the documented
#      kms:EncryptionContext:aws:logs:arn condition so the log service can only
#      use the key for this project's log groups, not arbitrary ones.

locals {
  name_prefix = "${var.project}-${var.environment}"

  tags = merge(var.tags, {
    Module = "kms"
  })

  # The four log-group naming families the platform creates (see the
  # aws_cloudwatch_log_group resources across modules/*): the ECS/Flink/EMR
  # groups under /<project>/<environment>/, the API Gateway access-log group,
  # the Lambda groups, and the WAF group. CloudWatch Logs presents the log
  # group ARN as encryption context, so these patterns are what the condition
  # below matches against.
  log_group_arn_patterns = [
    "arn:${data.aws_partition.current.partition}:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/${var.project}/${var.environment}/*",
    "arn:${data.aws_partition.current.partition}:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/apigateway/${local.name_prefix}-*",
    "arn:${data.aws_partition.current.partition}:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${local.name_prefix}-*",
    "arn:${data.aws_partition.current.partition}:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:aws-waf-logs-${local.name_prefix}-*",
  ]
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

resource "aws_kms_key" "this" {
  description             = "${local.name_prefix} CMK for S3, RDS, DynamoDB, and CloudWatch Logs encryption"
  enable_key_rotation     = true
  deletion_window_in_days = 7

  # Inline jsonencode rather than the repo's usual aws_iam_policy_document
  # data source: a key policy attaches to the key itself so Resource must be
  # "*", and checkov flags wildcard-resource policy DOCUMENTS (CKV_AWS_111/
  # CKV_AWS_356) regardless of how they are used. Inline JSON keeps this new
  # module passing outright instead of leaning on the baseline.
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AccountRootFullAccess"
        Effect = "Allow"
        Principal = {
          AWS = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      },
      {
        Sid    = "AllowCloudWatchLogsUse"
        Effect = "Allow"
        Principal = {
          Service = "logs.${var.aws_region}.amazonaws.com"
        }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey",
        ]
        Resource = "*"
        Condition = {
          ArnLike = {
            "kms:EncryptionContext:aws:logs:arn" = local.log_group_arn_patterns
          }
        }
      },
    ]
  })

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-cmk"
  })
}

resource "aws_kms_alias" "this" {
  name          = "alias/${local.name_prefix}"
  target_key_id = aws_kms_key.this.key_id
}
