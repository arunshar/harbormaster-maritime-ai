# envs/base/main.tf
#
# Root configuration for the Harbormaster "base" environment. Wires the three
# Phase 0 modules together: network, state_stores, finops. This root is the only
# place a provider is configured; provider version constraints live in the
# shared ../../versions.tf (symlinked/loaded here via the terraform block below).
#
# Apply order note: build guardrails first. terraform apply creates the FinOps
# budgets and the teardown Lambda alongside the network and stores, so the $75
# cap and nightly sweep exist before any heavier Phase 1 compute is added.

terraform {
  required_version = ">= 1.9" # cross-variable validation (enable_phase2 -> enable_phase1)

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
    helm = {
      # 2.x pinned: the 3.x provider changed the kubernetes block to
      # attribute syntax, which would silently break the provider config
      # below on an unpinned init (war-story P8 policy).
      source  = "hashicorp/helm"
      version = "~> 2.13"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }
}

# -----------------------------------------------------------------------------
# Helm provider (Phase 5, gate 5.1), configured COUNT-SAFELY. A provider
# block cannot be count-gated, so the credentials come from data sources
# that only exist while enable_phase5_kubernetes_access = true (which per its
# validation also requires enable_phase5). With the flag off, both data sources have
# count 0, every try() below collapses to an inert empty value, and the
# provider is never dereferenced because the only helm_release
# (modules/eks_cluster's keda, gated on install_keda = enable_phase5_keda)
# has count 0 too: a default plan reads no live cluster credentials, and make
# validate needs no AWS credentials. With the flag on, the data sources read the LIVE cluster
# at plan time, which is why flipping it is a documented SECOND apply after
# the cluster exists (see the enable_phase5_keda variable description).
# -----------------------------------------------------------------------------

data "aws_eks_cluster" "phase5" {
  count = var.enable_phase5_kubernetes_access ? 1 : 0
  name  = local.phase5_cluster_name
}

data "aws_eks_cluster_auth" "phase5" {
  count = var.enable_phase5_kubernetes_access ? 1 : 0
  name  = local.phase5_cluster_name
}

provider "helm" {
  kubernetes {
    host  = try(data.aws_eks_cluster.phase5[0].endpoint, "")
    token = try(data.aws_eks_cluster_auth.phase5[0].token, "")
    cluster_ca_certificate = try(
      base64decode(data.aws_eks_cluster.phase5[0].certificate_authority[0].data),
      null,
    )
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.common_tags
  }
}

data "aws_caller_identity" "current" {}

locals {
  # Common tags applied to every resource across every module, per the shared
  # conventions. default_tags above also stamps these provider-wide; passing
  # them into modules keeps the tags explicit on resources that need name-scoping.
  common_tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
  }

  # ARN of the IAM permissions boundary (created by infra/aws/bootstrap.sh) that
  # every module-created role must carry once the boundary-gated harbormaster-
  # platform deploy policy is attached (war story P32, the two-sided contract).
  # Passed into every role-creating module below. Roles are count-gated on the
  # enable_phaseN flags, so the default phases-off plan creates none and is
  # unaffected. Set permissions_boundary_name = "" to opt out of the boundary.
  permissions_boundary_arn = var.permissions_boundary_name != "" ? "arn:aws:iam::${data.aws_caller_identity.current.account_id}:policy/${var.permissions_boundary_name}" : ""

  # CMK ARN passed to every encryption-capable module. Empty while enable_cmk
  # is false, and every consumer's kms_key_arn input collapses to its pre-CMK
  # configuration on empty, so the default plan stays a ZERO diff.
  kms_key_arn = var.enable_cmk ? module.kms[0].key_arn : ""

  # Foundation creation and application activation are separate gates. This
  # prevents a new Phase 2 ECR repository from being created in the same apply
  # that launches a service referencing an image it cannot yet contain.
  cdc_services_enabled = var.enable_phase2 && var.enable_cdc_services

  # The endpoint module is the sole owner of the Pi-DPM failure alert target.
  # This separate local makes the state-store EventBridge switch, the
  # endpoint-side rule, and the invocation-failure alarm move together only
  # when a real endpoint is configured.
  pidpm_endpoint_enabled = var.enable_phase3 && var.pidpm_image != "" && var.pidpm_model_data_url != ""
  pidpm_failure_alerts_enabled = (
    local.pidpm_endpoint_enabled && var.enable_pidpm_failure_alerts
  )
}

# -----------------------------------------------------------------------------
# Network
# -----------------------------------------------------------------------------

module "network" {
  source = "../../modules/network"

  project     = var.project
  environment = var.environment
  aws_region  = var.aws_region
  enable_nat  = var.enable_nat

  tags = local.common_tags
}

# -----------------------------------------------------------------------------
# State stores (lake + models buckets, Feast online + TF lock tables)
# -----------------------------------------------------------------------------

module "state_stores" {
  source = "../../modules/state_stores"

  project     = var.project
  environment = var.environment

  kms_key_arn               = local.kms_key_arn
  enable_models_eventbridge = local.pidpm_failure_alerts_enabled

  tags = local.common_tags
}

# -----------------------------------------------------------------------------
# Customer-managed KMS key (CMK). Count-gated on enable_cmk (default false):
# the key, its alias, and every consumer's kms_key_arn wiring are inert until
# the flag flips, keeping the default plan byte-identical to the pre-CMK
# configuration. Authored for the buyer-grade encryption path; NOT applied.
# -----------------------------------------------------------------------------

module "kms" {
  count  = var.enable_cmk ? 1 : 0
  source = "../../modules/kms"

  project     = var.project
  environment = var.environment
  aws_region  = var.aws_region

  tags = local.common_tags
}

# -----------------------------------------------------------------------------
# FinOps guardrails (budgets, anomaly detection, teardown Lambda)
# -----------------------------------------------------------------------------

module "finops" {
  source                   = "../../modules/finops"
  permissions_boundary_arn = local.permissions_boundary_arn

  project     = var.project
  environment = var.environment
  aws_region  = var.aws_region

  alert_email        = var.alert_email
  platform_role_name = var.platform_role_name

  enable_nightly_teardown = var.enable_nightly_teardown
  teardown_dry_run        = var.teardown_dry_run

  existing_cost_anomaly_monitor_arn = var.cost_anomaly_monitor_arn

  # Lambda source lives at infra/lambda/teardown relative to this root:
  # envs/base -> ../../.. = infra, then lambda/teardown.
  lambda_source_dir = "${path.module}/../../../lambda/teardown"

  tags = local.common_tags
}

# -----------------------------------------------------------------------------
# Phase 1 (gate 1.3): streaming + serving pipeline. Every module is gated behind
# enable_phase1 (default false) so a base apply stays Phase-0-only and cheap.
# Flip enable_phase1 = true for a demo apply, then set it back to false (or
# destroy the Phase 1 resources) to stop the billable compute. Data plane in
# this batch: kinesis, firehose, rds. Compute plane (ecs_*, kda_flink, apigw)
# follows in the next batch.
# -----------------------------------------------------------------------------

module "kinesis" {
  count  = var.enable_phase1 ? 1 : 0
  source = "../../modules/kinesis"

  project     = var.project
  environment = var.environment
  tags        = local.common_tags
}

module "firehose" {
  count                    = var.enable_phase1 ? 1 : 0
  source                   = "../../modules/firehose"
  permissions_boundary_arn = local.permissions_boundary_arn

  project     = var.project
  environment = var.environment

  kinesis_stream_arn = module.kinesis[0].stream_arn
  lake_bucket_arn    = "arn:aws:s3:::${module.state_stores.lake_bucket_name}"

  tags = local.common_tags
}

module "rds" {
  count  = var.enable_phase1 ? 1 : 0
  source = "../../modules/rds"

  project     = var.project
  environment = var.environment

  vpc_id             = module.network.vpc_id
  private_subnet_ids = module.network.private_subnet_ids
  # The VPC is 10.0.0.0/16 (network module default); allow in-VPC Postgres.
  allowed_ingress_cidrs = ["10.0.0.0/16"]

  # Logical decoding is distinct from the Phase 2 service switch: existing
  # replication slots must remain supported while MSK and CDC services stay off.
  # Enabling this attaches a custom parameter group and requires a separate,
  # reviewed reboot; it does not deploy Phase 2 workloads.
  logical_replication = var.rds_logical_replication_enabled

  allocated_storage_gb     = var.rds_allocated_storage_gb
  max_allocated_storage_gb = var.rds_max_allocated_storage_gb
  iops                     = var.rds_iops
  storage_throughput       = var.rds_storage_throughput

  kms_key_arn = local.kms_key_arn

  tags = local.common_tags
}

module "ecs_cluster" {
  count  = var.enable_phase1 ? 1 : 0
  source = "../../modules/ecs_cluster"

  project     = var.project
  environment = var.environment
  tags        = local.common_tags
}

module "ecs_serving" {
  count                    = var.enable_phase1 ? 1 : 0
  source                   = "../../modules/ecs_serving"
  permissions_boundary_arn = local.permissions_boundary_arn

  project     = var.project
  environment = var.environment
  aws_region  = var.aws_region

  vpc_id            = module.network.vpc_id
  public_subnet_ids = module.network.public_subnet_ids

  cluster_arn  = module.ecs_cluster[0].cluster_arn
  cluster_name = module.ecs_cluster[0].cluster_name

  feast_table_name = module.state_stores.feast_online_table_name
  lake_bucket_arn  = "arn:aws:s3:::${module.state_stores.lake_bucket_name}"
  rds_secret_arn   = module.rds[0].master_user_secret_arn
  rds_endpoint     = module.rds[0].db_endpoint
  serving_image    = var.serving_image
  # Plan-time-known bool (RDS and serving are both behind enable_phase1), so
  # the module's secret-policy count never depends on an apply-time RDS output.
  attach_rds_secret_policy = true

  # Phase 2 application activation: point the scorer at the CDC-fed online
  # store only after the reviewed CDC services are ready to supply it. The
  # Redis DNS name is deterministic, so no module reference is needed here.
  cdc_online_enabled = local.cdc_services_enabled
  redis_url          = local.cdc_services_enabled ? "redis://redis.${var.project}-${var.environment}.local:6379/0" : ""

  kms_key_arn = local.kms_key_arn

  tags = local.common_tags
}

module "apigw" {
  count  = var.enable_phase1 ? 1 : 0
  source = "../../modules/apigw"

  project     = var.project
  environment = var.environment

  vpc_id             = module.network.vpc_id
  private_subnet_ids = module.network.private_subnet_ids

  cloudmap_service_arn = module.ecs_serving[0].cloudmap_service_arn

  # Phase 5 keeps the ECS integration as rollback while adding a second
  # integration backed by the Terraform-owned internal NLB. At the default
  # target and with Phase 5 disabled, these values preserve the ECS route.
  serving_target         = var.serving_target
  enable_eks_integration = var.enable_phase5
  eks_integration_uri = (
    var.enable_phase5 ? module.eks_frontdoor[0].listener_arn : ""
  )
  eks_nlb_security_group_id = (
    var.enable_phase5 ? module.eks_frontdoor[0].security_group_id : ""
  )

  kms_key_arn = local.kms_key_arn

  tags = local.common_tags
}

module "ecs_ingestor" {
  count                    = var.enable_phase1 ? 1 : 0
  source                   = "../../modules/ecs_ingestor"
  permissions_boundary_arn = local.permissions_boundary_arn

  project     = var.project
  environment = var.environment
  aws_region  = var.aws_region

  vpc_id             = module.network.vpc_id
  kinesis_stream_arn = module.kinesis[0].stream_arn
  lake_bucket_arn    = "arn:aws:s3:::${module.state_stores.lake_bucket_name}"

  kms_key_arn = local.kms_key_arn

  tags = local.common_tags
}

module "kda_flink" {
  count                    = var.enable_phase1 ? 1 : 0
  source                   = "../../modules/kda_flink"
  permissions_boundary_arn = local.permissions_boundary_arn

  project     = var.project
  environment = var.environment
  aws_region  = var.aws_region

  kinesis_stream_arn        = module.kinesis[0].stream_arn
  kinesis_stream_name       = module.kinesis[0].stream_name
  feast_table_name          = module.state_stores.feast_online_table_name
  lake_bucket_arn           = "arn:aws:s3:::${module.state_stores.lake_bucket_name}"
  serving_endpoint          = module.apigw[0].api_endpoint
  serving_api_execution_arn = module.apigw[0].api_execution_arn

  # flink_code_s3_key stays empty until gate 1.5 uploads the job artifact, so the
  # Flink application (and its KPU cost) is not created by a 1.3 demo apply.
  # code_bucket_arn is the same lake bucket the artifact is uploaded to
  # (s3://<lake_bucket>/flink/flink-app.zip), not a separate bucket.
  code_bucket_arn   = "arn:aws:s3:::${module.state_stores.lake_bucket_name}"
  flink_code_s3_key = var.flink_code_s3_key

  kms_key_arn = local.kms_key_arn

  tags = local.common_tags
}

module "observability" {
  count  = var.enable_phase1 ? 1 : 0
  source = "../../modules/observability"

  project     = var.project
  environment = var.environment
  aws_region  = var.aws_region

  api_id              = module.apigw[0].api_id
  cluster_name        = module.ecs_cluster[0].cluster_name
  service_name        = module.ecs_serving[0].service_name
  kinesis_stream_name = module.kinesis[0].stream_name
  sns_topic_arn       = module.finops.sns_topic_arn

  tags = local.common_tags
}

# -----------------------------------------------------------------------------
# Phase 2 (gate 2.7): the CDC showcase plane. enable_phase2 creates the
# foundation (MSK, Redis, ECR, and monitoring) and requires enable_phase1 for
# RDS, the ECS cluster, and the Cloud Map namespace. enable_cdc_services is a
# second gate for Connect, the consumer, and serving's online-store wiring.
# MSK Serverless is ~$18/day while up, so demo windows only, then flip
# enable_phase2 back to false. The application modules also require non-empty
# image values so a foundation apply before ECR pushes creates no crash-looping
# services.
# -----------------------------------------------------------------------------

module "msk" {
  count  = var.enable_phase2 ? 1 : 0
  source = "../../modules/msk_serverless"

  project     = var.project
  environment = var.environment

  vpc_id     = module.network.vpc_id
  subnet_ids = module.network.private_subnet_ids

  tags = local.common_tags
}

module "redis_fargate" {
  count                    = var.enable_phase2 ? 1 : 0
  source                   = "../../modules/redis_fargate"
  permissions_boundary_arn = local.permissions_boundary_arn

  project     = var.project
  environment = var.environment

  vpc_id            = module.network.vpc_id
  public_subnet_ids = module.network.public_subnet_ids

  cluster_arn           = module.ecs_cluster[0].cluster_arn
  cloudmap_namespace_id = module.ecs_serving[0].cloudmap_namespace_id

  kms_key_arn = local.kms_key_arn

  tags = local.common_tags
}

# ECR repos for the two CDC images live OUTSIDE the image-gated service modules:
# they must exist before an image can be pushed, and the services can only be
# created after the push (apply -> push -> set image vars -> apply).
resource "aws_ecr_repository" "cdc_connect" {
  count = var.enable_phase2 ? 1 : 0

  name                 = "${var.project}-${var.environment}-cdc-connect"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = merge(local.common_tags, { Name = "${var.project}-${var.environment}-cdc-connect" })
}

resource "aws_ecr_repository" "cdc_consumer" {
  count = var.enable_phase2 ? 1 : 0

  name                 = "${var.project}-${var.environment}-cdc-consumer"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = merge(local.common_tags, { Name = "${var.project}-${var.environment}-cdc-consumer" })
}

module "ecs_connect" {
  count                    = local.cdc_services_enabled && var.cdc_connect_image != "" ? 1 : 0
  source                   = "../../modules/ecs_connect"
  permissions_boundary_arn = local.permissions_boundary_arn

  project     = var.project
  environment = var.environment
  aws_region  = var.aws_region

  vpc_id            = module.network.vpc_id
  public_subnet_ids = module.network.public_subnet_ids
  cluster_arn       = module.ecs_cluster[0].cluster_arn

  image = var.cdc_connect_image

  msk_cluster_arn        = module.msk[0].cluster_arn
  msk_topic_wildcard_arn = module.msk[0].topic_wildcard_arn
  msk_group_wildcard_arn = module.msk[0].group_wildcard_arn
  msk_bootstrap          = module.msk[0].bootstrap_brokers_sasl_iam

  rds_endpoint   = module.rds[0].db_endpoint
  rds_secret_arn = module.rds[0].master_user_secret_arn

  kms_key_arn = local.kms_key_arn

  tags = local.common_tags
}

module "ecs_cdc_consumer" {
  count                    = local.cdc_services_enabled && var.cdc_consumer_image != "" ? 1 : 0
  source                   = "../../modules/ecs_cdc_consumer"
  permissions_boundary_arn = local.permissions_boundary_arn

  project     = var.project
  environment = var.environment
  aws_region  = var.aws_region

  vpc_id            = module.network.vpc_id
  public_subnet_ids = module.network.public_subnet_ids
  cluster_arn       = module.ecs_cluster[0].cluster_arn

  image = var.cdc_consumer_image

  msk_cluster_arn        = module.msk[0].cluster_arn
  msk_topic_wildcard_arn = module.msk[0].topic_wildcard_arn
  msk_group_wildcard_arn = module.msk[0].group_wildcard_arn
  msk_bootstrap          = module.msk[0].bootstrap_brokers_sasl_iam

  feast_table_name = module.state_stores.feast_online_table_name
  lake_bucket_arn  = "arn:aws:s3:::${module.state_stores.lake_bucket_name}"
  redis_url        = "redis://${module.redis_fargate[0].redis_dns}:6379/0"

  kms_key_arn = local.kms_key_arn

  tags = local.common_tags
}

module "cdc_monitoring" {
  count                    = var.enable_phase2 ? 1 : 0
  source                   = "../../modules/cdc_monitoring"
  permissions_boundary_arn = local.permissions_boundary_arn

  project     = var.project
  environment = var.environment

  vpc_id             = module.network.vpc_id
  private_subnet_ids = module.network.private_subnet_ids

  rds_endpoint         = module.rds[0].db_endpoint
  rds_secret_arn       = module.rds[0].master_user_secret_arn
  slot_lag_alarm_bytes = var.slot_lag_alarm_bytes

  sns_topic_arn = module.finops.sns_topic_arn

  # Built by `make cdc-lambda-package` (handler + shared monitor + pg8000).
  lambda_source_dir = "${path.module}/../../../lambda/cdc_slot_lag/build"

  tags = local.common_tags
}

# -----------------------------------------------------------------------------
# Phase 3 (gate 3.2): the transient MarineCadastre lake backfill. Whole-module
# gate behind enable_phase3 (default false; requires enable_phase1 for the
# VPC and the lake bucket/Glue catalog Phase 0 already wires up). No job run
# is Terraform-managed (transient, submitted via `aws emr-serverless
# start-job-run`, operator-run, at demo-apply time); only the application + its
# execution role are standing infra, and the application itself auto-stops
# after idle_timeout_minutes with no job running.
# -----------------------------------------------------------------------------

module "emr_backfill" {
  count                    = var.enable_phase3 ? 1 : 0
  source                   = "../../modules/emr_backfill"
  permissions_boundary_arn = local.permissions_boundary_arn

  project     = var.project
  environment = var.environment
  aws_region  = var.aws_region

  raw_extract_s3_uri = "arn:aws:s3:::${module.state_stores.lake_bucket_name}/raw/marinecadastre"
  lake_bucket_arn    = "arn:aws:s3:::${module.state_stores.lake_bucket_name}"

  kms_key_arn = local.kms_key_arn

  tags = local.common_tags
}

# Phase 3 (gate 3.6): the Pi-DPM async endpoint. Additionally gated on both
# image vars being set (the flink_code_s3_key pattern): an apply before the
# container is built and the checkpoint is exported creates no
# half-configured model/endpoint.
module "sagemaker_pidpm" {
  count                    = local.pidpm_endpoint_enabled ? 1 : 0
  source                   = "../../modules/sagemaker_pidpm"
  permissions_boundary_arn = local.permissions_boundary_arn

  project     = var.project
  environment = var.environment
  aws_region  = var.aws_region

  container_image   = var.pidpm_image
  model_data_url    = var.pidpm_model_data_url
  models_bucket_arn = "arn:aws:s3:::${module.state_stores.models_bucket_name}"

  enable_failure_alerts = local.pidpm_failure_alerts_enabled
  failure_alert_email   = var.alert_email

  # Weighted-canary challenger (gate 3.7). Not part of the count gate above:
  # the candidate is optional inside the module (empty means single-variant),
  # while a module with no champion image/checkpoint is not an endpoint at all.
  candidate_model_data_url = var.pidpm_candidate_model_data_url

  tags = local.common_tags

  # The bucket notification must be active before the endpoint module creates
  # a rule that relies on model-bucket EventBridge events.
  depends_on = [module.state_stores]
}

# -----------------------------------------------------------------------------
# Phase 5 (gate 5.0): the EKS teardown guard, authored BEFORE the cluster it
# guards (gate 5.1) so the structural cost mitigation can never lag the cost.
# Whole-module gate behind enable_phase5 (default false; requires
# enable_phase1, the enable_phase2/3 convention). Zero-diff argument, from
# construction: this call is the ONLY enable_phase5 reference in the plan at
# gate 5.0, the module's count collapses it entirely at the default, and no
# pre-existing resource or module input anywhere in this root changed, so the
# enable_phase5 = false plan is byte-identical to the pre-gate configuration.
# The guarded cluster name is a deterministic local, never a module output,
# so the guard has no Terraform dependency on the cluster it destroys.
# -----------------------------------------------------------------------------

locals {
  # Single source of truth for the Phase 5 cluster name, shared by the guard
  # (gate 5.0) and modules/eks_cluster (gate 5.1) so they can never disagree.
  phase5_cluster_name = "${var.project}-${var.environment}-eks"
}

module "eks_teardown_guard" {
  count                    = var.enable_phase5 ? 1 : 0
  source                   = "../../modules/eks_teardown_guard"
  permissions_boundary_arn = local.permissions_boundary_arn

  project     = var.project
  environment = var.environment
  aws_region  = var.aws_region

  cluster_name        = local.phase5_cluster_name
  max_age_hours       = var.phase5_teardown_max_age_hours
  guard_dry_run       = var.phase5_guard_dry_run
  schedule_expression = var.phase5_teardown_schedule_expression

  sns_topic_arn = module.finops.sns_topic_arn

  # Lambda source at infra/lambda/eks_teardown relative to this root, the
  # finops teardown packaging convention (boto3-only, zipped as-is).
  lambda_source_dir = "${path.module}/../../../lambda/eks_teardown"

  tags = local.common_tags
}

# -----------------------------------------------------------------------------
# Phase 5 (gate 5.1): the EKS control plane + scale-to-zero spot node group.
# Whole-module gates behind enable_phase5 (validated to require
# enable_phase1: the cluster sits in the Phase 1 VPC's private subnets).
# Zero-diff argument, from construction: both calls collapse at the default,
# the helm provider's data sources collapse behind
# enable_phase5_kubernetes_access, and
# no pre-existing resource or module input changed, so the toggles-off plan
# stays byte-identical. COST WATCH: the control plane bills ~$0.10/hour from
# creation regardless of nodes/pods; the gate 5.0 guard above force-destroys
# it after phase5_teardown_max_age_hours unless phase5_keep_alive_until
# stamps a future KeepAliveUntil tag. The node group starts at desired 0
# (no EC2 instance, no compute cost) and the demo runbook owns the bump.
# NOT applied during authoring: validate + structural tests only, per the
# no-credentials rule; the isolated -target plan add-count is pinned at the
# W4 demo window.
# -----------------------------------------------------------------------------

module "eks_cluster" {
  count                    = var.enable_phase5 ? 1 : 0
  source                   = "../../modules/eks_cluster"
  permissions_boundary_arn = local.permissions_boundary_arn

  project     = var.project
  environment = var.environment
  aws_region  = var.aws_region

  cluster_name       = local.phase5_cluster_name
  vpc_id             = module.network.vpc_id
  private_subnet_ids = module.network.private_subnet_ids

  endpoint_public_access = var.phase5_endpoint_public_access
  public_access_cidrs    = var.phase5_public_access_cidrs

  keep_alive_until = var.phase5_keep_alive_until

  # The two-step KEDA install (see enable_phase5_keda's description): the
  # helm_release inside the module is gated on this. The helm provider's
  # credential data sources use the separate Kubernetes-access flag above so
  # KEDA can be removed cleanly before cluster teardown.
  install_keda = var.enable_phase5_keda

  tags = local.common_tags

  # Both guards must be wet before the first billable control-plane minute.
  # The dedicated EKS guard is created first, and the FinOps dependency makes
  # any nightly teardown Lambda or IAM update finish before EKS creation.
  # These dependencies reverse safely on teardown: EKS is removed first.
  depends_on = [module.eks_teardown_guard, module.finops]
}

module "eks_node_group" {
  count  = var.enable_phase5 ? 1 : 0
  source = "../../modules/eks_node_group"

  project     = var.project
  environment = var.environment

  # Referencing the cluster module's outputs (not the deterministic name
  # local) is deliberate here: it orders node-group creation after the
  # control plane and ties the node role to the module that owns EKS IAM.
  cluster_name  = module.eks_cluster[0].cluster_name
  node_role_arn = module.eks_cluster[0].node_role_arn

  private_subnet_ids              = module.network.private_subnet_ids
  vpc_id                          = module.network.vpc_id
  vpc_cidr                        = module.network.vpc_cidr
  control_plane_security_group_id = module.eks_cluster[0].control_plane_security_group_id

  desired_size = var.phase5_node_desired_size

  tags = local.common_tags
}

# Terraform-owned internal NLB for the EKS NodePort Service. Its listener ARN
# feeds the existing API Gateway VPC link directly, so W4 has no out-of-band
# load-balancer controller, ARN discovery, or untracked billable resource.
module "eks_frontdoor" {
  count  = var.enable_phase5 ? 1 : 0
  source = "../../modules/eks_frontdoor"

  project     = var.project
  environment = var.environment

  vpc_id             = module.network.vpc_id
  private_subnet_ids = module.network.private_subnet_ids

  node_security_group_id      = module.eks_node_group[0].node_security_group_id
  node_autoscaling_group_name = module.eks_node_group[0].autoscaling_group_name

  tags = local.common_tags
}

# Phase 4 (gate 4.6): the drift-alerting plane. Depends only on Phase 0 (the
# lake bucket + the finops SNS topic), not Phase 1/3, so it is gated purely
# on enable_phase4 with no enable_phase1 requirement, unlike Phase 2/3. NOT
# applied during the 2026-07-04 sprint: authored, validate + plan-checksum
# verified only, per docs/phases/PHASE_4.md.
module "drift_watch" {
  count                    = var.enable_phase4 ? 1 : 0
  source                   = "../../modules/drift_watch"
  permissions_boundary_arn = local.permissions_boundary_arn

  project     = var.project
  environment = var.environment

  lake_bucket_arn  = "arn:aws:s3:::${module.state_stores.lake_bucket_name}"
  lake_bucket_name = module.state_stores.lake_bucket_name
  sns_topic_arn    = module.finops.sns_topic_arn

  # Existing Phase 3 MarineCadastre raw extracts. The small demo source uses
  # a one-row floor, so any future production source must raise that floor in
  # this reviewed configuration rather than silently accepting it.
  snapshot_source_prefix = "raw/marinecadastre/"
  snapshot_window_prefix = "drift/windows/"
  snapshot_min_rows      = 1
  snapshot_max_rows      = 10000

  lambda_artifacts_bucket_name = module.state_stores.models_bucket_name

  lambda_source_dir = "${path.module}/../../../lambda/drift_watch/build"

  kms_key_arn = local.kms_key_arn

  tags = local.common_tags
}
