# modules/state_stores/variables.tf

variable "project" {
  description = "Project name, used in tags and bucket/table names."
  type        = string
  default     = "harbormaster"
}

variable "environment" {
  description = "Deployment environment: base or demo."
  type        = string

  validation {
    condition     = contains(["base", "demo"], var.environment)
    error_message = "environment must be one of: base, demo."
  }
}

variable "lake_noncurrent_expiration_days" {
  description = "Days after which noncurrent (versioned) lake object versions expire."
  type        = number
  default     = 30
}

variable "raw_transition_ia_days" {
  description = "Days after which raw/ objects transition to S3 Standard-IA to cut storage cost."
  type        = number
  default     = 30
}

variable "abort_multipart_days" {
  description = "Days after which incomplete multipart uploads are aborted (avoids paying for orphaned parts)."
  type        = number
  default     = 7
}

variable "feast_online_table_ttl_enabled" {
  description = "Whether to enable a TTL attribute on the Feast online DynamoDB table."
  type        = bool
  default     = true
}

variable "kms_key_arn" {
  description = "ARN of the customer-managed KMS key for bucket and table encryption. Empty (the default) keeps S3 on SSE-AES256 and DynamoDB on its AWS-owned key, so the default plan stays a zero diff."
  type        = string
  default     = ""
}

variable "enable_models_eventbridge" {
  description = "Enable EventBridge delivery for model-bucket object events. Disabled by default and enabled only with the reviewed Pi-DPM async-failure alert path."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Common tags applied to every resource."
  type        = map(string)
  default     = {}
}
