# modules/ecs_ingestor/main.tf
#
# The replay ingestor as a Fargate task definition (run on demand, no standing
# service or cost). It reads the synthetic replay fixture from the lake and PutRecords to
# the ais-raw Kinesis stream. Run it in a PUBLIC subnet with a public IP (see the
# ecs_serving note) so it reaches Kinesis / ECR / Logs over the IGW, no NAT.

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

variable "kinesis_stream_arn" {
  type = string
}

variable "lake_bucket_arn" {
  type = string
}

variable "cpu" {
  type    = number
  default = 256
}

variable "memory" {
  type    = number
  default = 512
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
  service     = "ingestor"
  tags        = merge(var.tags, { Module = "ecs_ingestor" })
}

resource "aws_ecr_repository" "ingestor" {
  name                 = "${local.name_prefix}-ingestor"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = merge(local.tags, { Name = "${local.name_prefix}-ingestor" })
}

resource "aws_cloudwatch_log_group" "ingestor" {
  name              = "/harbormaster/${var.environment}/ingestor"
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
  name                 = "${local.name_prefix}-ingestor-exec"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null
  assume_role_policy   = data.aws_iam_policy_document.task_assume.json
  tags                 = local.tags
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "task" {
  name                 = "${local.name_prefix}-ingestor-task"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null
  assume_role_policy   = data.aws_iam_policy_document.task_assume.json
  tags                 = local.tags
}

data "aws_iam_policy_document" "task" {
  statement {
    sid    = "WriteStream"
    effect = "Allow"
    actions = [
      "kinesis:PutRecord",
      "kinesis:PutRecords",
      "kinesis:DescribeStream",
    ]
    resources = [var.kinesis_stream_arn]
  }

  statement {
    sid    = "ReadFixture"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      var.lake_bucket_arn,
      "${var.lake_bucket_arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "task" {
  name   = "${local.name_prefix}-ingestor-task"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task.json
}

resource "aws_security_group" "ingestor" {
  name        = "${local.name_prefix}-ingestor-sg"
  description = "Ingestor egress to Kinesis/S3 over the IGW"
  vpc_id      = var.vpc_id

  egress {
    description = "all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${local.name_prefix}-ingestor-sg" })
}

resource "aws_ecs_task_definition" "ingestor" {
  family                   = "${local.name_prefix}-ingestor"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.cpu
  memory                   = var.memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([
    {
      name      = local.service
      image     = "${aws_ecr_repository.ingestor.repository_url}:latest"
      essential = true
      environment = [
        { name = "AWS_REGION", value = var.aws_region },
        { name = "KINESIS_STREAM_ARN", value = var.kinesis_stream_arn },
        { name = "MODE", value = "REPLAY" },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.ingestor.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = local.service
        }
      }
    }
  ])

  tags = local.tags
}

output "task_definition_arn" {
  value = aws_ecs_task_definition.ingestor.arn
}

output "ecr_repository_url" {
  value = aws_ecr_repository.ingestor.repository_url
}

output "security_group_id" {
  value = aws_security_group.ingestor.id
}
