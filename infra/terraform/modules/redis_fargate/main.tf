# modules/redis_fargate/main.tf
#
# The CDC invalidation cache as a tiny containerized Redis on Fargate Spot with
# Cloud Map DNS (Phase 2, gate C7). Deliberately NOT ElastiCache: at ~$9-12/mo
# always-on, ElastiCache loses to a demo-window container under the $75 cap
# (the documented production answer stays ElastiCache; same trade-off shape as
# Phase 1's ALB -> API Gateway decision). The cache is invalidation-only state
# (a lost cache repopulates from DynamoDB via the serving read-through), so an
# ephemeral, storage-free container is architecturally honest here, not a hack.

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
  type    = string
  default = "10.0.0.0/16"
}

variable "public_subnet_ids" {
  type = list(string)
}

variable "cluster_arn" {
  type = string
}

variable "cloudmap_namespace_id" {
  description = "The ecs_serving private DNS namespace; redis registers into it."
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
  service     = "redis"
  tags        = merge(var.tags, { Module = "redis_fargate" })
}

resource "aws_cloudwatch_log_group" "redis" {
  name              = "/harbormaster/${var.environment}/redis"
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
  name                 = "${local.name_prefix}-redis-exec"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null
  assume_role_policy   = data.aws_iam_policy_document.task_assume.json
  tags                 = local.tags
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_security_group" "redis" {
  name        = "${local.name_prefix}-redis-sg"
  description = "Redis 6379 from in-VPC (serving lookup + CDC consumer)"
  vpc_id      = var.vpc_id

  ingress {
    description = "redis from in-VPC"
    from_port   = 6379
    to_port     = 6379
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

  tags = merge(local.tags, { Name = "${local.name_prefix}-redis-sg" })
}

resource "aws_service_discovery_service" "redis" {
  name = local.service

  dns_config {
    namespace_id = var.cloudmap_namespace_id

    dns_records {
      type = "A"
      ttl  = 10
    }

    routing_policy = "MULTIVALUE"
  }

  health_check_custom_config {
    failure_threshold = 1
  }

  tags = local.tags
}

resource "aws_ecs_task_definition" "redis" {
  family                   = "${local.name_prefix}-redis"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.execution.arn

  container_definitions = jsonencode([
    {
      name      = local.service
      image     = "public.ecr.aws/docker/library/redis:7-alpine"
      essential = true
      portMappings = [
        { containerPort = 6379, protocol = "tcp" }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.redis.name
          awslogs-region        = data.aws_region.current.name
          awslogs-stream-prefix = local.service
        }
      }
    }
  ])

  tags = local.tags
}

data "aws_region" "current" {}

resource "aws_ecs_service" "redis" {
  name            = "${local.name_prefix}-redis"
  cluster         = var.cluster_arn
  task_definition = aws_ecs_task_definition.redis.arn
  desired_count   = 1

  capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 1
  }

  network_configuration {
    # Public subnets + public IP for image pull over the IGW (no NAT); the SG
    # allows inbound only from the VPC CIDR (the ecs_serving pattern).
    subnets          = var.public_subnet_ids
    security_groups  = [aws_security_group.redis.id]
    assign_public_ip = true
  }

  service_registries {
    registry_arn = aws_service_discovery_service.redis.arn
  }

  tags = local.tags
}

output "redis_dns" {
  description = "In-VPC Redis endpoint (Cloud Map)."
  value       = "${local.service}.${var.project}-${var.environment}.local"
}

output "security_group_id" {
  value = aws_security_group.redis.id
}
