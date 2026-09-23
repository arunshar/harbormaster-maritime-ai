# modules/ecs_connect/main.tf
#
# Debezium Kafka Connect on Fargate (Phase 2, gate C7): the same
# quay.io/debezium/connect image as the local plane, plus the aws-msk-iam-auth
# jar (built by cdc/connect/Dockerfile, pushed to the ECR repo envs/base
# creates behind enable_phase2). Worker state (configs/offsets/status) lives in MSK topics, so the
# service is stateless and demo-window disposable; a connector restart resumes
# from the replication slot, which is exactly acceptance test 2.9(c).
#
# The Postgres password reaches the worker as an ECS-injected secret (from the
# RDS-managed Secrets Manager secret). The entrypoint copies it to tmpfs and the
# connector config references it through DirectoryConfigProvider, so no
# credential lands in the Connect REST config history or this repo.
#
# Registering the connector at demo time (the REST port is in-VPC only):
#   aws ecs execute-command into the task, or a one-off curl task in the VPC,
#   POSTing the body from cdc/connector/config.py build_connector_config().

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

variable "image" {
  description = "The cdc/connect image URI (Debezium + aws-msk-iam-auth), pushed to ECR."
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

variable "rds_endpoint" {
  type = string
}

variable "rds_secret_arn" {
  type = string
}

variable "cpu" {
  type    = number
  default = 1024
}

variable "memory" {
  type    = number
  default = 2048
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
  service     = "connect"
  tags        = merge(var.tags, { Module = "ecs_connect" })

  # The debezium/connect entrypoint maps CONNECT_* env vars onto worker props.
  msk_sasl = {
    "security.protocol"                  = "SASL_SSL"
    "sasl.mechanism"                     = "AWS_MSK_IAM"
    "sasl.jaas.config"                   = "software.amazon.msk.auth.iam.IAMLoginModule required;"
    "sasl.client.callback.handler.class" = "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
  }
}

resource "aws_cloudwatch_log_group" "connect" {
  name              = "/harbormaster/${var.environment}/cdc-connect"
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
  name                 = "${local.name_prefix}-cdc-connect-exec"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null
  assume_role_policy   = data.aws_iam_policy_document.task_assume.json
  tags                 = local.tags
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# The EXECUTION role fetches the injected secret (ECS secrets contract).
data "aws_iam_policy_document" "execution_secrets" {
  statement {
    sid       = "ReadPgSecret"
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.rds_secret_arn]
  }
}

resource "aws_iam_role_policy" "execution_secrets" {
  name   = "${local.name_prefix}-cdc-connect-secrets"
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.execution_secrets.json
}

resource "aws_iam_role" "task" {
  name                 = "${local.name_prefix}-cdc-connect-task"
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
    sid    = "MskTopics"
    effect = "Allow"
    actions = [
      "kafka-cluster:CreateTopic",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:ReadData",
      "kafka-cluster:WriteData",
    ]
    resources = [var.msk_topic_wildcard_arn]
  }

  statement {
    sid    = "MskGroups"
    effect = "Allow"
    actions = [
      "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeGroup",
    ]
    resources = [var.msk_group_wildcard_arn]
  }

  statement {
    sid    = "EcsExecSsmChannel"
    effect = "Allow"
    actions = [
      "ssmmessages:CreateControlChannel",
      "ssmmessages:CreateDataChannel",
      "ssmmessages:OpenControlChannel",
      "ssmmessages:OpenDataChannel",
    ]
    # ssmmessages actions do not support resource-level scoping
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "task" {
  name   = "${local.name_prefix}-cdc-connect-task"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task.json
}

resource "aws_security_group" "connect" {
  name        = "${local.name_prefix}-cdc-connect-sg"
  description = "Connect REST 8083 from in-VPC only; egress to MSK/RDS/ECR"
  vpc_id      = var.vpc_id

  ingress {
    description = "connect REST from in-VPC"
    from_port   = 8083
    to_port     = 8083
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

  tags = merge(local.tags, { Name = "${local.name_prefix}-cdc-connect-sg" })
}

resource "aws_ecs_task_definition" "connect" {
  family                   = "${local.name_prefix}-cdc-connect"
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
      # Secret-to-file bridge (Wave 4 live window, 2026-07-12). The database
      # password must reach the connector config WITHOUT landing in the repo or
      # the Connect config-storage topic (only the ${dir:...} placeholder does).
      # Fargate has no native Secrets Manager file volume, so a shell entrypoint
      # wrapper writes the ECS-injected secret to tmpfs before the JVM launches,
      # then hands off to the stock Debezium entrypoint. The connector
      # references it via DirectoryConfigProvider (${dir:/dev/shm/secrets:password}),
      # which returns the whole file's bytes verbatim (no properties/escaping
      # pitfalls). printf '%s' writes no trailing newline, so the value is exact.
      # /dev/shm is tmpfs (in-memory, never on disk).
      #
      # The W3 registration failure was transport-side: the runbook's unquoted
      # heredoc expanded the ${dir:...} placeholder in Bash before Connect saw
      # it. cdc.connector.registration now emits a base64 transport command.
      entryPoint = ["/bin/sh", "-c"]
      command = [
        "umask 077; mkdir -p /dev/shm/secrets; printf '%s' \"$HM_PG_PASSWORD\" > /dev/shm/secrets/password; exec /docker-entrypoint.sh start"
      ]
      portMappings = [
        { containerPort = 8083, protocol = "tcp" }
      ]
      environment = concat(
        [
          { name = "BOOTSTRAP_SERVERS", value = var.msk_bootstrap },
          { name = "GROUP_ID", value = "hm-connect" },
          { name = "CONFIG_STORAGE_TOPIC", value = "hm-connect-configs" },
          { name = "OFFSET_STORAGE_TOPIC", value = "hm-connect-offsets" },
          { name = "STATUS_STORAGE_TOPIC", value = "hm-connect-status" },
          { name = "HM_PG_HOST", value = var.rds_endpoint },
          { name = "CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", value = "false" },
          { name = "CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", value = "false" },
          # DirectoryConfigProvider reads /dev/shm/secrets/<key> (whole-file
          # value) for ${dir:/dev/shm/secrets:<key>} references. The file is
          # written by the entrypoint wrapper above from the ECS-injected secret.
          { name = "CONNECT_CONFIG_PROVIDERS", value = "dir" },
          {
            name  = "CONNECT_CONFIG_PROVIDERS_DIR_CLASS",
            value = "org.apache.kafka.common.config.provider.DirectoryConfigProvider"
          },
        ],
        # worker + embedded producer/consumer all authenticate to MSK via IAM
        flatten([
          for prefix in ["CONNECT", "CONNECT_PRODUCER", "CONNECT_CONSUMER"] : [
            for k, v in local.msk_sasl : {
              name  = "${prefix}_${upper(replace(k, ".", "_"))}",
              value = v
            }
          ]
        ])
      )
      secrets = [
        {
          name      = "HM_PG_PASSWORD",
          valueFrom = "${var.rds_secret_arn}:password::"
        }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.connect.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = local.service
        }
      }
    }
  ])

  tags = local.tags
}

resource "aws_ecs_service" "connect" {
  name            = "${local.name_prefix}-cdc-connect"
  cluster         = var.cluster_arn
  task_definition = aws_ecs_task_definition.connect.arn
  desired_count   = var.desired_count

  # The documented connector-registration path is `aws ecs execute-command`
  # into this task (the REST port is in-VPC only); exec needs this flag plus
  # the ssmmessages permissions on the task role.
  enable_execute_command = true

  capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 1
  }

  network_configuration {
    # Public subnets + public IP for ECR/Logs egress over the IGW (no NAT);
    # the SG allows inbound only from the VPC CIDR (the ecs_serving pattern).
    subnets          = var.public_subnet_ids
    security_groups  = [aws_security_group.connect.id]
    assign_public_ip = true
  }

  tags = local.tags
}

output "service_name" {
  value = aws_ecs_service.connect.name
}

output "security_group_id" {
  value = aws_security_group.connect.id
}
