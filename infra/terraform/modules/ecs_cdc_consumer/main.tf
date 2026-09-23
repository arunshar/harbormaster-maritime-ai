# modules/ecs_cdc_consumer/main.tf
#
# The LSN-guarded CDC consumer on Fargate (Phase 2, gate C7): the
# cdc/consumer/Dockerfile image (pushed to the ECR repo created here), reading
# the Debezium topics from MSK Serverless via IAM and writing the online
# stores: conditional PutItem into the Phase 0 feast_online DynamoDB table,
# Redis invalidation (redis_fargate via Cloud Map), and the Iceberg cdc_audit
# table on the lake bucket through the Glue catalog (Athena-queryable).
# Stateless by design: offsets live in Kafka, the idempotency guard lives in
# the DynamoDB items, so kill/replace is free (acceptance 2.9(b)/(c)).

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

variable "vpc_id" {
  type = string
}

variable "public_subnet_ids" {
  type = list(string)
}

variable "cluster_arn" {
  type = string
}

variable "image" {
  description = "The cdc/consumer image URI, pushed to ECR."
  type        = string
}

variable "msk_cluster_arn" {
  type = string
}

variable "msk_topic_wildcard_arn" {
  type = string
}

variable "msk_group_wildcard_arn" {
  type = string
}

variable "msk_bootstrap" {
  type = string
}

variable "feast_table_name" {
  type = string
}

variable "lake_bucket_arn" {
  type = string
}

variable "redis_url" {
  type = string
}

variable "cpu" {
  type    = number
  default = 256
}

variable "memory" {
  type    = number
  default = 1024
}

variable "desired_count" {
  type    = number
  default = 1
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
  service     = "cdc-consumer"
  tags        = merge(var.tags, { Module = "ecs_cdc_consumer" })
  lake_bucket = replace(var.lake_bucket_arn, "arn:aws:s3:::", "")
}

data "aws_caller_identity" "current" {}

resource "aws_cloudwatch_log_group" "consumer" {
  name              = "/harbormaster/${var.environment}/cdc-consumer"
  retention_in_days = var.log_retention_days
  # CMK when set; null keeps the CloudWatch Logs default encryption (zero diff).
  kms_key_id = var.kms_key_arn != "" ? var.kms_key_arn : null
  tags       = local.tags
}

data "aws_iam_policy_document" "task_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name                 = "${local.name_prefix}-cdc-consumer-exec"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null
  assume_role_policy   = data.aws_iam_policy_document.task_assume.json
  tags                 = local.tags
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "task" {
  name                 = "${local.name_prefix}-cdc-consumer-task"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null
  assume_role_policy   = data.aws_iam_policy_document.task_assume.json
  tags                 = local.tags
}

data "aws_iam_policy_document" "task" {
  statement {
    sid    = "MskConnect"
    effect = "Allow"
    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeCluster",
    ]
    resources = [var.msk_cluster_arn]
  }

  statement {
    sid    = "MskConsume"
    effect = "Allow"
    actions = [
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:ReadData",
    ]
    resources = [var.msk_topic_wildcard_arn]
  }

  statement {
    sid    = "MskGroup"
    effect = "Allow"
    actions = [
      "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeGroup",
    ]
    resources = [var.msk_group_wildcard_arn]
  }

  statement {
    sid    = "OnlineStoreConditionalWrites"
    effect = "Allow"
    actions = [
      "dynamodb:PutItem",
      "dynamodb:GetItem",
      "dynamodb:Query",
    ]
    resources = [
      "arn:aws:dynamodb:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${var.feast_table_name}"
    ]
  }

  statement {
    sid    = "IcebergAuditLake"
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
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/hm",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/hm/*",
    ]
  }
}

resource "aws_iam_role_policy" "task" {
  name   = "${local.name_prefix}-cdc-consumer-task"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task.json
}

resource "aws_security_group" "consumer" {
  name        = "${local.name_prefix}-cdc-consumer-sg"
  description = "CDC consumer egress to MSK/DynamoDB/S3/Redis over the IGW"
  vpc_id      = var.vpc_id

  egress {
    description = "all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${local.name_prefix}-cdc-consumer-sg" })
}

resource "aws_ecs_task_definition" "consumer" {
  family                   = "${local.name_prefix}-cdc-consumer"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.cpu
  memory                   = var.memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([
    {
      name      = local.service
      image     = var.image
      essential = true
      environment = [
        { name = "HM_KAFKA_BOOTSTRAP", value = var.msk_bootstrap },
        { name = "HM_KAFKA_MSK_IAM", value = "1" },
        { name = "HM_ONLINE_TABLE", value = var.feast_table_name },
        { name = "HM_REDIS_URL", value = var.redis_url },
        { name = "HM_ICEBERG_GLUE", value = "1" },
        { name = "HM_ICEBERG_WAREHOUSE", value = "s3://${local.lake_bucket}/iceberg" },
        { name = "HM_METRICS_PORT", value = "9400" },
        { name = "AWS_REGION", value = var.aws_region },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.consumer.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = local.service
        }
      }
    }
  ])

  tags = local.tags
}

resource "aws_ecs_service" "consumer" {
  name            = "${local.name_prefix}-cdc-consumer"
  cluster         = var.cluster_arn
  task_definition = aws_ecs_task_definition.consumer.arn
  desired_count   = var.desired_count

  capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 1
  }

  network_configuration {
    # Public subnets + public IP for egress over the IGW (the ecs_serving
    # pattern); no inbound port at all, the SG is egress-only.
    subnets          = var.public_subnet_ids
    security_groups  = [aws_security_group.consumer.id]
    assign_public_ip = true
  }

  tags = local.tags
}

output "service_name" {
  value = aws_ecs_service.consumer.name
}
