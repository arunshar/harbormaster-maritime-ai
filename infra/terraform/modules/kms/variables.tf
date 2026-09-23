# modules/kms/variables.tf

variable "project" {
  description = "Project name, used in tags and resource names."
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

variable "aws_region" {
  description = "AWS region, used for the CloudWatch Logs service principal and the log-group encryption-context patterns in the key policy."
  type        = string
  default     = "us-east-1"
}

variable "tags" {
  description = "Common tags applied to every resource."
  type        = map(string)
  default     = {}
}
