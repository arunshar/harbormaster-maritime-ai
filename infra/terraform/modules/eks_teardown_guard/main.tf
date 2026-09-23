# modules/eks_teardown_guard
#
# The STRUCTURAL cost guardrail for the EKS control plane. It was written
# before any EKS resource existed, so the guard can never lag the cluster it
# guards. The control plane is the only compute surface in the project whose
# idle cost cannot be scaled to zero, because it bills with zero nodes and
# zero pods. Switching the feature off after a demo is a procedural step, and
# this guard makes the teardown structural. Mirroring modules/finops's nightly-teardown philosophy (guardrails
# as code, not a checklist): a recurring EventBridge Scheduler schedule
# invokes a Lambda (infra/lambda/eks_teardown/handler.py, packaged exactly
# like modules/finops's teardown Lambda: archive_file zips the source dir,
# boto3 from the runtime, nothing bundled) that force-destroys the node
# groups and then the cluster once it is older than max_age_hours (default
# 4), unless the cluster's KeepAliveUntil tag holds a future ISO 8601
# timestamp. An unparseable tag grants no extension: the guard fails toward
# teardown, never toward an immortal control plane.
#
# The whole module is count-gated at the envs/base call site
# (count = var.enable_phase5 ? 1 : 0), the modules/emr_backfill convention,
# so the enable_phase5 = false plan is a zero diff by construction: no
# resource outside this module is touched, and none inside it exists.
#
# Checkov posture (new resources pass outright, never baselined): a
# module-local CMK encrypts the Lambda environment, the log group, and the
# schedule payload; the function carries X-Ray tracing and an SNS dead-letter
# target. It deliberately uses account-level unreserved concurrency because
# accounts at the 10-execution quota floor cannot reserve even one execution.
# The deliberate exceptions carry inline skips with reasons, matching the
# repo's narrow per-line nosec discipline rather than the baseline.

locals {
  name_prefix = "${var.project}-${var.environment}"

  tags = merge(var.tags, {
    Module = "eks_teardown_guard"
  })

  # The guarded cluster's name. Deterministic (never a module output) so the
  # guard has zero Terraform dependency on the cluster it destroys: the guard
  # can exist before, after, and without the cluster.
  cluster_name = var.cluster_name != "" ? var.cluster_name : "${local.name_prefix}-eks"

  function_name  = "${local.name_prefix}-eks-teardown-guard"
  log_group_name = "/aws/lambda/${local.name_prefix}-eks-teardown-guard"

  cluster_arn   = "arn:${data.aws_partition.current.partition}:eks:${var.aws_region}:${data.aws_caller_identity.current.account_id}:cluster/${local.cluster_name}"
  nodegroup_arn = "arn:${data.aws_partition.current.partition}:eks:${var.aws_region}:${data.aws_caller_identity.current.account_id}:nodegroup/${local.cluster_name}/*/*"
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

# -----------------------------------------------------------------------------
# Module-local CMK: Lambda env vars, the log group, and the schedule payload.
# Inline jsonencode rather than aws_iam_policy_document, the modules/kms
# precedent: a key policy attaches to the key itself so Resource must be "*",
# and checkov flags wildcard-resource policy DOCUMENTS regardless of use.
# -----------------------------------------------------------------------------

resource "aws_kms_key" "guard" {
  description             = "${local.name_prefix} CMK for the EKS teardown guard (Lambda env, logs, schedule)"
  enable_key_rotation     = true
  deletion_window_in_days = 7

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
            "kms:EncryptionContext:aws:logs:arn" = [
              "arn:${data.aws_partition.current.partition}:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:${local.log_group_name}",
            ]
          }
        }
      },
    ]
  })

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-eks-guard-cmk"
  })
}

resource "aws_kms_alias" "guard" {
  name          = "alias/${local.name_prefix}-eks-guard"
  target_key_id = aws_kms_key.guard.key_id
}

# -----------------------------------------------------------------------------
# Lambda packaging: zip the source dir at plan time, the modules/finops
# teardown convention (the source lives outside the module; the caller passes
# lambda_source_dir, see envs/base/main.tf).
# -----------------------------------------------------------------------------

data "archive_file" "guard" {
  type        = "zip"
  source_dir  = var.lambda_source_dir
  output_path = "${path.module}/.build/eks-teardown-guard-${var.environment}.zip"
}

# -----------------------------------------------------------------------------
# Lambda execution role: least privilege, every statement resource-scoped.
# -----------------------------------------------------------------------------

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

resource "aws_iam_role" "guard" {
  name                 = "${local.name_prefix}-eks-teardown-guard"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null
  assume_role_policy   = data.aws_iam_policy_document.lambda_assume.json

  tags = local.tags
}

data "aws_iam_policy_document" "guard" {
  statement {
    sid    = "Logs"
    effect = "Allow"

    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]

    resources = [
      "arn:${data.aws_partition.current.partition}:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:${local.log_group_name}:*",
    ]
  }

  # Exactly the calls the handler makes, scoped to exactly the guarded
  # cluster and its node groups. No eks:* and no Resource "*": the guard can
  # destroy the one cluster it exists for and nothing else.
  statement {
    sid    = "EksDescribeAndDestroyGuardedCluster"
    effect = "Allow"

    actions = [
      "eks:DescribeCluster",
      "eks:ListNodegroups",
      "eks:DescribeNodegroup",
      "eks:DeleteNodegroup",
      "eks:DeleteCluster",
    ]

    resources = [
      local.cluster_arn,
      local.nodegroup_arn,
    ]
  }

  statement {
    sid    = "SnsSummaryAndDlq"
    effect = "Allow"

    actions   = ["sns:Publish"]
    resources = [var.sns_topic_arn]
  }

  statement {
    sid    = "KmsDecryptEnv"
    effect = "Allow"

    actions   = ["kms:Decrypt"]
    resources = [aws_kms_key.guard.arn]
  }
}

resource "aws_iam_role_policy" "guard" {
  name   = "${local.name_prefix}-eks-teardown-guard"
  role   = aws_iam_role.guard.id
  policy = data.aws_iam_policy_document.guard.json
}

# X-Ray write access for the Active tracing below. AWS managed policy because
# xray:PutTraceSegments/PutTelemetryRecords support no resource-level scoping.
resource "aws_iam_role_policy_attachment" "guard_xray" {
  role       = aws_iam_role.guard.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AWSXRayDaemonWriteAccess"
}

resource "aws_cloudwatch_log_group" "guard" {
  name              = local.log_group_name
  retention_in_days = var.log_retention_days
  kms_key_id        = aws_kms_key.guard.arn
  tags              = local.tags
}

resource "aws_lambda_function" "guard" {
  # checkov:skip=CKV_AWS_117:Deliberately not VPC-attached. The handler calls only the EKS/SNS management APIs on public AWS endpoints; the platform runs with enable_nat = false, so a VPC attachment would leave the guard with no egress and silently disarm it, the exact failure it exists to prevent (mirrors modules/drift_watch's documented no-VPC stance).
  # checkov:skip=CKV_AWS_272:No code-signing pipeline exists in this repo. The artifact is first-party source zipped from infra/lambda/eks_teardown by archive_file and pinned by source_code_hash; a Signer profile with a Warn-on-untrusted policy would satisfy the check without enforcing anything, which is check-gaming, not security.
  # checkov:skip=CKV_AWS_115:Accounts at the 10-execution Lambda quota floor cannot reserve one execution because AWS requires all 10 to remain unreserved. The recurring 30-minute schedule is much slower than the 120-second timeout, and the teardown handler converges safely on AWS retries.
  function_name = local.function_name
  role          = aws_iam_role.guard.arn
  handler       = "handler.lambda_handler"
  runtime       = var.lambda_runtime
  timeout       = var.lambda_timeout_seconds

  filename         = data.archive_file.guard.output_path
  source_code_hash = data.archive_file.guard.output_base64sha256

  kms_key_arn = aws_kms_key.guard.arn

  tracing_config {
    mode = "Active"
  }

  dead_letter_config {
    target_arn = var.sns_topic_arn
  }

  # Env-var names match infra/lambda/eks_teardown/handler.py's contract
  # exactly. DRY_RUN default false HERE (unlike the handler's own safe local
  # default): an armed guard is this module's entire purpose; guard_dry_run
  # exists for a rehearsal window, not as a resting state.
  environment {
    variables = {
      CLUSTER_NAME       = local.cluster_name
      MAX_AGE_HOURS      = tostring(var.max_age_hours)
      KEEP_ALIVE_TAG_KEY = var.keep_alive_tag_key
      DRY_RUN            = tostring(var.guard_dry_run)
      PROJECT_TAG        = var.project
      ALERT_TOPIC_ARN    = var.sns_topic_arn
    }
  }

  depends_on = [aws_cloudwatch_log_group.guard]

  tags = local.tags
}

# -----------------------------------------------------------------------------
# Recurring schedule. A rate schedule that re-evaluates, not a one-shot at()
# computed at apply time: the decision (age vs keep-alive) lives in the
# Lambda's tested pure function, so extending a demo is a tag update, never a
# schedule rewrite, and a missed tick self-heals on the next one.
# -----------------------------------------------------------------------------

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
  name                 = "${local.name_prefix}-eks-guard-scheduler"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null
  assume_role_policy   = data.aws_iam_policy_document.scheduler_assume.json

  tags = local.tags
}

data "aws_iam_policy_document" "scheduler_invoke" {
  statement {
    effect    = "Allow"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.guard.arn]
  }

  # The schedule encrypts its target payload with the module CMK below.
  statement {
    effect    = "Allow"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.guard.arn]
  }
}

resource "aws_iam_role_policy" "scheduler_invoke" {
  name   = "${local.name_prefix}-eks-guard-scheduler-invoke"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler_invoke.json
}

resource "aws_scheduler_schedule" "guard" {
  name  = "${local.name_prefix}-eks-teardown-guard"
  state = "ENABLED"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = "UTC"

  kms_key_arn = aws_kms_key.guard.arn

  target {
    arn      = aws_lambda_function.guard.arn
    role_arn = aws_iam_role.scheduler.arn
  }
}

resource "aws_lambda_permission" "allow_scheduler" {
  statement_id  = "AllowExecutionFromScheduler"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.guard.function_name
  principal     = "scheduler.amazonaws.com"
  source_arn    = aws_scheduler_schedule.guard.arn
}
