# envs/base/outputs.tf
#
# Surface the values Phase 1 components (streaming, CDC, serving) will need to
# reference, plus the FinOps control-plane identifiers.

# ---- Network ----------------------------------------------------------------

output "vpc_id" {
  description = "ID of the Harbormaster VPC."
  value       = module.network.vpc_id
}

output "public_subnet_ids" {
  description = "Public subnet IDs, one per AZ."
  value       = module.network.public_subnet_ids
}

output "private_subnet_ids" {
  description = "Private subnet IDs, one per AZ."
  value       = module.network.private_subnet_ids
}

output "s3_vpc_endpoint_id" {
  description = "S3 gateway VPC endpoint ID."
  value       = module.network.s3_vpc_endpoint_id
}

output "dynamodb_vpc_endpoint_id" {
  description = "DynamoDB gateway VPC endpoint ID."
  value       = module.network.dynamodb_vpc_endpoint_id
}

# ---- State stores -----------------------------------------------------------

output "lake_bucket_name" {
  description = "Data-lake S3 bucket name (holds raw/, iceberg/, features/)."
  value       = module.state_stores.lake_bucket_name
}

output "models_bucket_name" {
  description = "Model-artifacts S3 bucket name."
  value       = module.state_stores.models_bucket_name
}

output "feast_online_table_name" {
  description = "Feast online-store DynamoDB table name."
  value       = module.state_stores.feast_online_table_name
}

output "tf_state_lock_table_name" {
  description = "Terraform state-lock DynamoDB table name (for the optional S3 backend)."
  value       = module.state_stores.tf_state_lock_table_name
}

# ---- FinOps -----------------------------------------------------------------

output "budget_alerts_sns_topic_arn" {
  description = "SNS topic ARN that receives budget and cost-anomaly alerts."
  value       = module.finops.sns_topic_arn
}

output "spend_freeze_policy_arn" {
  description = "ARN of the deny policy attached to the platform role on $75 breach."
  value       = module.finops.spend_freeze_policy_arn
}

output "teardown_lambda_name" {
  description = "Name of the nightly teardown Lambda."
  value       = module.finops.teardown_lambda_name
}

# ---- Phase 1 (null when enable_phase1 = false) ------------------------------

output "kinesis_stream_name" {
  description = "Name of the ais-raw Kinesis stream (Phase 1)."
  value       = one(module.kinesis[*].stream_name)
}

output "firehose_delivery_stream_name" {
  description = "Name of the ais-raw -> S3 Firehose delivery stream (Phase 1)."
  value       = one(module.firehose[*].delivery_stream_name)
}

output "rds_endpoint" {
  description = "Postgres endpoint address (Phase 1)."
  value       = one(module.rds[*].db_endpoint)
}

output "rds_master_secret_arn" {
  description = "Secrets Manager ARN of the RDS-managed master credentials (Phase 1)."
  value       = one(module.rds[*].master_user_secret_arn)
}

output "serving_api_endpoint" {
  description = "API Gateway HTTP API invoke URL for the scorer (Phase 1)."
  value       = one(module.apigw[*].api_endpoint)
}

output "serving_ecr_repository_url" {
  description = "ECR repo URL for the serving image (Phase 1)."
  value       = one(module.ecs_serving[*].ecr_repository_url)
}

output "serving_cloudmap_dns" {
  description = "In-VPC DNS name for the scorer (Phase 1)."
  value       = one(module.ecs_serving[*].cloudmap_dns_name)
}

output "ingestor_task_definition_arn" {
  description = "Replay ingestor Fargate task definition ARN (Phase 1)."
  value       = one(module.ecs_ingestor[*].task_definition_arn)
}

output "ingestor_ecr_repository_url" {
  description = "ECR repo URL for the ingestor image (Phase 1; the module always exposed this, the env never surfaced it, found while prepping the W1 showcase runbook)."
  value       = one(module.ecs_ingestor[*].ecr_repository_url)
}

output "flink_role_arn" {
  description = "Managed Flink service execution role ARN (Phase 1)."
  value       = one(module.kda_flink[*].role_arn)
}

output "flink_application_name" {
  description = "Managed Flink application name, or null until flink_code_s3_key creates it (Phase 1)."
  value       = one(module.kda_flink[*].application_name)
}

output "phase1_dashboard_name" {
  description = "CloudWatch dashboard for the Phase 1 slice (Phase 1)."
  value       = one(module.observability[*].dashboard_name)
}

output "msk_bootstrap_sasl_iam" {
  description = "MSK Serverless IAM bootstrap brokers (Phase 2)."
  value       = one(module.msk[*].bootstrap_brokers_sasl_iam)
}

output "cdc_connect_ecr_repository_url" {
  description = "ECR repo for the Debezium Connect image (Phase 2; exists before the service)."
  value       = one(aws_ecr_repository.cdc_connect[*].repository_url)
}

output "cdc_consumer_ecr_repository_url" {
  description = "ECR repo for the CDC consumer image (Phase 2; exists before the service)."
  value       = one(aws_ecr_repository.cdc_consumer[*].repository_url)
}

output "cdc_redis_dns" {
  description = "In-VPC Redis endpoint for the CDC cache (Phase 2)."
  value       = one(module.redis_fargate[*].redis_dns)
}

output "cdc_slot_lag_alarm_name" {
  description = "CloudWatch alarm on replication-slot lag (Phase 2)."
  value       = one(module.cdc_monitoring[*].alarm_name)
}

# ---- Phase 3 (null when enable_phase3 = false) ------------------------------

output "emr_backfill_application_id" {
  description = "EMR Serverless application id for the transient MarineCadastre backfill (Phase 3). Job runs are submitted against this id, operator-run, not Terraform-managed."
  value       = one(module.emr_backfill[*].application_id)
}

output "emr_backfill_execution_role_arn" {
  description = "IAM role EMR Serverless job runs assume for this application (Phase 3)."
  value       = one(module.emr_backfill[*].execution_role_arn)
}

output "pidpm_endpoint_name" {
  description = "SageMaker async endpoint name for the Pi-DPM head (Phase 3, gate 3.6). Feed this to HM_PIDPM_ENDPOINT."
  value       = one(module.sagemaker_pidpm[*].endpoint_name)
}

output "pidpm_failure_alert_topic_arn" {
  description = "Dedicated SNS topic for opt-in Pi-DPM asynchronous failure artifacts, or null while the alert path is disabled."
  value       = one(module.sagemaker_pidpm[*].failure_alert_topic_arn)
}

output "pidpm_invocation_failure_alarm_name" {
  description = "CloudWatch alarm for opt-in Pi-DPM asynchronous invocation failures, or null while the alert path is disabled."
  value       = one(module.sagemaker_pidpm[*].invocation_failure_alarm_name)
}


# ---- Phase 5 (null when enable_phase5 = false) ------------------------------

output "phase5_cluster_name" {
  description = "EKS cluster name watched by the teardown guard."
  value       = one(module.eks_cluster[*].cluster_name)
}

output "phase5_node_group_name" {
  description = "Managed EKS serving node-group name."
  value       = one(module.eks_node_group[*].node_group_name)
}

output "phase5_keda_operator_role_arn" {
  description = "KEDA operator IRSA role ARN."
  value       = one(module.eks_cluster[*].keda_operator_role_arn)
}

output "phase5_teardown_guard_function_name" {
  description = "EKS teardown-guard Lambda function name."
  value       = one(module.eks_teardown_guard[*].function_name)
}

output "phase5_teardown_guard_schedule_name" {
  description = "EventBridge Scheduler schedule that invokes the EKS teardown guard."
  value       = one(module.eks_teardown_guard[*].schedule_name)
}

output "phase5_nlb_listener_arn" {
  description = "Internal NLB listener ARN wired into the API Gateway EKS integration."
  value       = one(module.eks_frontdoor[*].listener_arn)
}

output "phase5_nlb_dns_name" {
  description = "Internal NLB DNS name for diagnostic reads from inside the VPC."
  value       = one(module.eks_frontdoor[*].load_balancer_dns_name)
}
