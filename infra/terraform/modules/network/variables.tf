# modules/network/variables.tf

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
  description = "AWS region the VPC is created in. Used to look up availability zones."
  type        = string
  default     = "us-east-1"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.0.0.0/16"
}

variable "az_count" {
  description = "Number of availability zones to spread subnets across."
  type        = number
  default     = 2
}

variable "enable_nat" {
  description = <<-EOT
    Whether to create a single NAT gateway for private-subnet egress.
    Default false to avoid the hourly NAT charge plus data processing, which is
    the largest line item that can blow a $30 budget in an idle account. S3 and
    DynamoDB traffic uses the free gateway VPC endpoints regardless, so most of
    Phase 0 needs no NAT at all. Flip to true only when a private workload needs
    general internet egress (for example pulling container images at runtime).
  EOT
  type        = bool
  default     = false
}

variable "tags" {
  description = "Common tags applied to every resource."
  type        = map(string)
  default     = {}
}
