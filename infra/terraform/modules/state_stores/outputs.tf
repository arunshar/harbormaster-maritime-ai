# modules/state_stores/outputs.tf

output "lake_bucket_name" {
  description = "Name of the data-lake S3 bucket (holds raw/, iceberg/, features/)."
  value       = aws_s3_bucket.lake.bucket
}

output "lake_bucket_arn" {
  description = "ARN of the data-lake S3 bucket."
  value       = aws_s3_bucket.lake.arn
}

output "models_bucket_name" {
  description = "Name of the model-artifacts S3 bucket."
  value       = aws_s3_bucket.models.bucket
}

output "models_bucket_arn" {
  description = "ARN of the model-artifacts S3 bucket."
  value       = aws_s3_bucket.models.arn
}

output "feast_online_table_name" {
  description = "Name of the Feast online-store DynamoDB table."
  value       = aws_dynamodb_table.feast_online.name
}

output "feast_online_table_arn" {
  description = "ARN of the Feast online-store DynamoDB table."
  value       = aws_dynamodb_table.feast_online.arn
}

output "tf_state_lock_table_name" {
  description = "Name of the Terraform state-lock DynamoDB table (for the optional S3 backend)."
  value       = aws_dynamodb_table.tf_state_lock.name
}

output "tf_state_lock_table_arn" {
  description = "ARN of the Terraform state-lock DynamoDB table."
  value       = aws_dynamodb_table.tf_state_lock.arn
}
