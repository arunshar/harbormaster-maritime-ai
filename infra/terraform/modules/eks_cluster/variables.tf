# modules/eks_cluster/variables.tf

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
  description = "AWS region, used to build the scoped logs ARN in the module CMK's key policy."
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = <<-EOT
    Name for the EKS cluster. Empty (the default) derives
    "<project>-<environment>-eks". envs/base passes its phase5_cluster_name
    local so this always matches the name modules/eks_teardown_guard watches;
    the guard reads the name, never this module's outputs, so it survives
    this cluster's destruction.
  EOT
  type        = string
  default     = ""
}

variable "eks_version" {
  description = "EKS Kubernetes version, pinned per the war-story P8 policy (an unpinned version would let AWS's default roll the control plane under us between applies)."
  type        = string
  default     = "1.34"
}

variable "private_subnet_ids" {
  description = "Phase 1 VPC private subnets for the control-plane ENIs (the gate 5.1 spec: private API endpoint in the Phase 1 VPC)."
  type        = list(string)
}

variable "vpc_id" {
  description = "Phase 1 VPC for the Terraform-owned EKS control-plane bridge security group."
  type        = string
}

variable "endpoint_public_access" {
  description = <<-EOT
    Whether the API endpoint is also reachable publicly. Default false (the
    authored posture: private endpoint only). A laptop-driven demo window may
    flip this true in tfvars WITH a tight public_access_cidrs allowlist,
    because helm/kubectl cannot reach a private-only endpoint from outside
    the VPC; flip it back after the window.
  EOT
  type        = bool
  default     = false

  validation {
    condition = (
      var.endpoint_public_access
      ? length(var.public_access_cidrs) > 0
      : length(var.public_access_cidrs) == 0
    )
    error_message = "endpoint_public_access requires at least one public_access_cidrs host CIDR when true and an empty list when false."
  }
}

variable "public_access_cidrs" {
  description = "Operator host CIDRs for the IPv4 public endpoint. W4 accepts only IPv4 /32 entries and rejects world-open ranges."
  type        = list(string)
  default     = []

  validation {
    condition = alltrue([
      for cidr in var.public_access_cidrs :
      can(cidrhost(cidr, 0)) &&
      can(regex("^([0-9]{1,3}\\.){3}[0-9]{1,3}/32$", cidr))
    ])
    error_message = "public_access_cidrs entries must be valid IPv4 operator host CIDRs (/32), never world-open ranges."
  }
}

variable "keep_alive_until" {
  description = <<-EOT
    ISO 8601 UTC timestamp stamped on the cluster as the keep-alive tag the
    gate 5.0 teardown guard consults (e.g. "2026-07-12T02:00:00Z"). Empty
    (the default) stamps nothing, so the guard's max_age_hours window is the
    only clock. Unparseable values grant NO extension by the guard's
    fail-toward-teardown contract.
  EOT
  type        = string
  default     = ""
}

variable "keep_alive_tag_key" {
  description = "Tag key for the keep-alive timestamp; must match the guard's keep_alive_tag_key."
  type        = string
  default     = "KeepAliveUntil"
}

variable "install_keda" {
  description = <<-EOT
    Install KEDA via helm_release. Default false, and DELIBERATELY separate
    from the module being instantiated: the helm provider reads live cluster
    credentials at plan time (envs/base configures it from
    aws_eks_cluster/aws_eks_cluster_auth data sources gated on
    enable_phase5_keda), so this flips true only on a SECOND apply after the
    cluster exists. Documented limitation, not an accident: a provider block
    cannot be count-gated, and configuring helm from this module's own
    outputs inside the creating apply is the classic flaky chicken-and-egg.
  EOT
  type        = bool
  default     = false
}

variable "keda_chart_version" {
  description = "KEDA Helm chart version, pinned to a release tested with the EKS Kubernetes version."
  type        = string
  default     = "2.20.0"
}

variable "keda_namespace" {
  description = "Namespace the KEDA chart installs into."
  type        = string
  default     = "keda"
}

variable "keda_service_account_name" {
  description = "KEDA operator service account bound to the module's IRSA role."
  type        = string
  default     = "keda-operator"
}

variable "log_retention_days" {
  description = "Retention for the control-plane log group. 365 so the audit trail outlives any demo season."
  type        = number
  default     = 365
}

variable "permissions_boundary_arn" {
  description = "ARN of the IAM permissions boundary to attach to roles this module creates. Empty attaches no boundary. The harbormaster-platform deploy policy requires the harbormaster-permissions-boundary on every managed role (see war story P32, the two-sided contract), so envs/base sets this at apply time."
  type        = string
  default     = ""
}

variable "tags" {
  description = "Common tags applied to every resource."
  type        = map(string)
  default     = {}
}
