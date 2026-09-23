# modules/cdc_monitoring/main.tf
#
# pg_replication_slots lag alerting (Phase 2, gate C7; the alarm side of war
# story P1). EventBridge (1 min) -> a VPC-attached Lambda that queries the RDS
# instance and publishes Harbormaster/CDC ReplicationSlotLagBytes per slot ->
# a CloudWatch alarm to the FinOps SNS topic on sustained lag. Missing data is
# also alarmed: if the Lambda itself dies, silence must not look like health.
#
# BEFORE plan/apply with enable_phase2=true, run `make cdc-lambda-package`:
# it copies handler.py + the shared cdc/monitor/slot_lag.py into build/ and
# vendors pg8000 (pure Python), which is the directory archived below.

variable "project" {
  type    = string
  default = "harbormaster"
}

variable "environment" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "vpc_cidr" {
  description = "VPC CIDR, same default as every other in-VPC-ingress module (ecs_connect, ecs_serving, msk_serverless, redis_fargate)."
  type        = string
  default     = "10.0.0.0/16"
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "rds_endpoint" {
  type = string
}

variable "rds_port" {
  type    = number
  default = 5432
}

variable "rds_db_name" {
  type    = string
  default = "harbormaster"
}

variable "rds_secret_arn" {
  type = string
}

variable "slot_name" {
  description = "The Debezium slot the alarm watches (cdc/schema/ddl.py SLOT_NAME)."
  type        = string
  default     = "harbormaster_cdc"
}

variable "slot_lag_alarm_bytes" {
  description = "Alarm threshold on ReplicationSlotLagBytes (mirrors cdc/monitor DEFAULT_LAG_ALARM_BYTES)."
  type        = number
  default     = 209715200 # 200 MB
}

variable "sns_topic_arn" {
  type = string
}

variable "lambda_source_dir" {
  description = "The packaged build dir (see make cdc-lambda-package)."
  type        = string
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
  tags        = merge(var.tags, { Module = "cdc_monitoring" })
}

data "archive_file" "slot_lag" {
  type        = "zip"
  source_dir  = var.lambda_source_dir
  output_path = "${path.module}/.build/cdc-slot-lag.zip"
}

resource "aws_security_group" "slot_lag" {
  name        = "${local.name_prefix}-cdc-slotlag-sg"
  description = "Slot-lag Lambda egress to RDS 5432 + AWS APIs"
  vpc_id      = var.vpc_id

  egress {
    description = "all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${local.name_prefix}-cdc-slotlag-sg" })
}

# The Lambda sits in NAT-less private subnets (the platform's no-NAT cost
# posture), and the network module provides only S3/DynamoDB GATEWAY endpoints,
# so Secrets Manager and CloudWatch need INTERFACE endpoints or the Lambda can
# reach neither. Two endpoints ~ $0.02/hr, inside enable_phase2 demo windows
# only; still far cheaper than a NAT gateway.
#
# private_dns_enabled = true below means these endpoints hijack DNS resolution
# for secretsmanager./monitoring.<region>.amazonaws.com VPC-WIDE, not just for
# the slot-lag Lambda: ANY resource in the VPC (any subnet, any security
# group) that resolves those hostnames gets routed to this endpoint's ENI.
# Scoping this SG's ingress to only the Lambda's own SG (the original,
# narrower version of this rule) silently broke ecs_connect's Debezium
# container: it fetches its RDS password from Secrets Manager via ECS's
# native `secrets` task-definition field before the container even starts,
# got its DNS silently redirected here by the same private-DNS mechanism, and
# then timed out because its own SG wasn't in the allow-list (Wave 4 live
# window, 2026-07-12: ResourceInitializationError / "context deadline
# exceeded" on GetSecretValue). CIDR-based in-VPC ingress, matching the
# convention every other module in this repo already uses for the same
# reason (ecs_connect, ecs_serving, msk_serverless, redis_fargate), so any
# future in-VPC consumer of Secrets Manager/CloudWatch keeps working too.
resource "aws_security_group" "vpce" {
  name = "${local.name_prefix}-cdc-vpce-sg"
  # NOTE: `description` is immutable on aws_security_group (any change forces
  # a destroy+recreate, which then deadlocks here: the two VPC endpoints'
  # ENIs are still attached to the OLD group until THEIR OWN update runs, so
  # the destroy fails with DependencyViolation). Left as the original text on
  # purpose, only the ingress rule below actually needs to change and that
  # alone is a clean in-place update (a security group's ingress rules are
  # mutable via the API without recreating the group).
  description = "HTTPS to the Secrets Manager / CloudWatch interface endpoints"
  vpc_id      = var.vpc_id

  ingress {
    description = "443 from in-VPC (private-DNS endpoints resolve VPC-wide, not just for the slot-lag Lambda)"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    description = "all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${local.name_prefix}-cdc-vpce-sg" })
}

data "aws_region" "current" {}

resource "aws_vpc_endpoint" "secretsmanager" {
  vpc_id              = var.vpc_id
  service_name        = "com.amazonaws.${data.aws_region.current.name}.secretsmanager"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = var.private_subnet_ids
  security_group_ids  = [aws_security_group.vpce.id]
  private_dns_enabled = true

  tags = merge(local.tags, { Name = "${local.name_prefix}-cdc-vpce-secrets" })
}

resource "aws_vpc_endpoint" "monitoring" {
  vpc_id              = var.vpc_id
  service_name        = "com.amazonaws.${data.aws_region.current.name}.monitoring"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = var.private_subnet_ids
  security_group_ids  = [aws_security_group.vpce.id]
  private_dns_enabled = true

  tags = merge(local.tags, { Name = "${local.name_prefix}-cdc-vpce-monitoring" })
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

resource "aws_iam_role" "slot_lag" {
  name                 = "${local.name_prefix}-cdc-slotlag"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null
  assume_role_policy   = data.aws_iam_policy_document.lambda_assume.json
  tags                 = local.tags
}

resource "aws_iam_role_policy_attachment" "vpc_access" {
  role       = aws_iam_role.slot_lag.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

data "aws_iam_policy_document" "slot_lag" {
  statement {
    sid       = "ReadPgSecret"
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.rds_secret_arn]
  }

  statement {
    sid     = "PublishMetrics"
    effect  = "Allow"
    actions = ["cloudwatch:PutMetricData"]
    # PutMetricData does not support resource ARNs; scope by namespace instead.
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["Harbormaster/CDC"]
    }
  }
}

resource "aws_iam_role_policy" "slot_lag" {
  name   = "${local.name_prefix}-cdc-slotlag"
  role   = aws_iam_role.slot_lag.id
  policy = data.aws_iam_policy_document.slot_lag.json
}

resource "aws_lambda_function" "slot_lag" {
  function_name    = "${local.name_prefix}-cdc-slot-lag"
  role             = aws_iam_role.slot_lag.arn
  runtime          = "python3.12"
  handler          = "handler.handler"
  filename         = data.archive_file.slot_lag.output_path
  source_code_hash = data.archive_file.slot_lag.output_base64sha256
  timeout          = 30
  memory_size      = 128

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.slot_lag.id]
  }

  environment {
    variables = {
      PG_HOST       = var.rds_endpoint
      PG_PORT       = tostring(var.rds_port)
      PG_DB         = var.rds_db_name
      PG_SECRET_ARN = var.rds_secret_arn
    }
  }

  tags = merge(local.tags, { Name = "${local.name_prefix}-cdc-slot-lag" })
}

resource "aws_cloudwatch_event_rule" "every_minute" {
  name                = "${local.name_prefix}-cdc-slot-lag"
  description         = "Publish pg_replication_slots lag metrics every minute"
  schedule_expression = "rate(1 minute)"
  tags                = local.tags
}

resource "aws_cloudwatch_event_target" "slot_lag" {
  rule = aws_cloudwatch_event_rule.every_minute.name
  arn  = aws_lambda_function.slot_lag.arn
}

resource "aws_lambda_permission" "events" {
  statement_id  = "AllowEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.slot_lag.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.every_minute.arn
}

resource "aws_cloudwatch_metric_alarm" "slot_lag" {
  alarm_name        = "${local.name_prefix}-cdc-slot-lag"
  alarm_description = "CDC replication slot lag: a stalled consumer is pinning WAL (war story P1)"

  namespace   = "Harbormaster/CDC"
  metric_name = "ReplicationSlotLagBytes"
  dimensions = {
    SlotName = var.slot_name
  }

  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 3
  threshold           = var.slot_lag_alarm_bytes
  comparison_operator = "GreaterThanOrEqualToThreshold"

  # If the Lambda stops publishing, silence must page too.
  treat_missing_data        = "breaching"
  alarm_actions             = [var.sns_topic_arn]
  ok_actions                = [var.sns_topic_arn]
  insufficient_data_actions = [var.sns_topic_arn]

  tags = merge(local.tags, { Name = "${local.name_prefix}-cdc-slot-lag" })
}

output "lambda_function_name" {
  value = aws_lambda_function.slot_lag.function_name
}

output "alarm_name" {
  value = aws_cloudwatch_metric_alarm.slot_lag.alarm_name
}
