# modules/eks_frontdoor/variables.tf

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

variable "vpc_id" {
  description = "VPC containing the EKS nodes and API Gateway VPC link."
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnets for the internal Network Load Balancer."
  type        = list(string)
}

variable "node_security_group_id" {
  description = "Dedicated worker-node security group that receives the NLB-only NodePort rule."
  type        = string
}

variable "node_autoscaling_group_name" {
  description = "Managed node group's underlying Auto Scaling group."
  type        = string
}

variable "node_port" {
  description = "Fixed Kubernetes NodePort exposed by the serving Service."
  type        = number
  default     = 30080

  validation {
    condition     = var.node_port >= 30000 && var.node_port <= 32767
    error_message = "node_port must be in Kubernetes' default NodePort range 30000..32767."
  }
}

variable "tags" {
  description = "Common tags applied to every resource."
  type        = map(string)
  default     = {}
}
