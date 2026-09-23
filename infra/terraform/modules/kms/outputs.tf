# modules/kms/outputs.tf

output "key_arn" {
  description = "ARN of the customer-managed KMS key; what envs/base passes to the consumer modules' kms_key_arn inputs."
  value       = aws_kms_key.this.arn
}

output "key_id" {
  description = "Key id of the customer-managed KMS key."
  value       = aws_kms_key.this.key_id
}

output "alias_name" {
  description = "Alias of the key (alias/<project>-<environment>)."
  value       = aws_kms_alias.this.name
}
