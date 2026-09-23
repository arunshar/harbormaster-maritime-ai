# modules/apigw/variables.tf
#
# Inputs for the serving HTTP API and its hardening controls (throttling, access
# logging, authorization, WAF). Every hardening variable defaults to a safe,
# no-new-spend posture so envs/base validates unchanged and nothing costly is
# forced on at rest.

variable "project" {
  type    = string
  default = "harbormaster"
}

variable "environment" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "cloudmap_service_arn" {
  type = string
}

variable "kms_key_arn" {
  description = "ARN of the customer-managed KMS key for log-group encryption. Empty (the default) keeps the CloudWatch Logs default encryption, so the default plan stays a zero diff."
  type        = string
  default     = ""
}

variable "tags" {
  type    = map(string)
  default = {}
}

# --- Throttling (stage-level default route settings) -------------------------
# Non-zero defaults so the public endpoint is never unbounded. These bound
# requests per second and the burst bucket for the $default stage's default
# route. They do not add cost; they only cap throughput.

variable "throttling_rate_limit" {
  description = "Steady-state request rate cap (requests/second) for the default route."
  type        = number
  default     = 50
}

variable "throttling_burst_limit" {
  description = "Burst bucket size (requests) for the default route."
  type        = number
  default     = 100
}

# --- Access logging ----------------------------------------------------------
# A dedicated CloudWatch log group receives structured JSON access logs. A
# 14-day retention keeps storage cheap. This is a few cents of cost at low
# volume, so it is opt-out via a flag but defaults on for auditability.

variable "enable_access_logging" {
  description = "Emit structured JSON access logs to CloudWatch for the stage."
  type        = bool
  default     = true
}

variable "access_log_retention_days" {
  description = "CloudWatch log-group retention (days) for API access logs."
  type        = number
  default     = 14
}

# --- Authorization -----------------------------------------------------------
# authorization_mode picks the posture for the proxy route:
#   "AWS_IAM" (default) - SigV4 IAM authorization. Free for HTTP APIs; callers
#                         must sign requests with IAM credentials. Safe default:
#                         the route is not anonymously open.
#   "JWT"     - JWT authorizer (e.g. Cognito / any OIDC issuer). Requires
#               jwt_issuer and jwt_audience; also free (no Lambda invoke cost).
#   "NONE"    - explicitly anonymous. Author-only escape hatch; not recommended.
# Defaulting to AWS_IAM closes the "no authorizer" gap without new spend and
# without standing up an identity provider.

variable "authorization_mode" {
  description = "Route authorization posture: AWS_IAM, JWT, or NONE."
  type        = string
  default     = "AWS_IAM"

  validation {
    condition     = contains(["AWS_IAM", "JWT", "NONE"], var.authorization_mode)
    error_message = "authorization_mode must be one of: AWS_IAM, JWT, NONE."
  }
}

variable "jwt_issuer" {
  description = "OIDC issuer URL for the JWT authorizer (required when authorization_mode = JWT)."
  type        = string
  default     = ""
}

variable "jwt_audience" {
  description = "Allowed audiences (client IDs) for the JWT authorizer (required when authorization_mode = JWT)."
  type        = list(string)
  default     = []
}

# --- Phase 5 (gate 5.2): EKS front-door retarget, authored-not-cutover ------
# The EKS migration retargets the proxy route from the ECS Cloud Map
# integration to a second integration over the SAME VPC Link. Both defaults
# below keep the shipped configuration byte-identical (serving_target =
# "ecs", no EKS integration exists), so the ECS path stays the intact,
# documented rollback path until a live demo proves the EKS one; flipping
# serving_target is a one-variable route retarget, and flipping it back is
# the rollback.
#
# LIMITATION, stated honestly: an HTTP API VPC Link integration can only
# target an ELB listener or a Cloud Map service ARN, never a bare
# cluster-internal DNS name, so PHASE_5.md's "retargets ... to the new EKS
# Service's cluster-internal DNS" is realized as eks_integration_uri = the
# ARN of a Cloud Map service (or internal NLB listener) that fronts the EKS
# Service, registered by the demo runbook.

variable "enable_eks_integration" {
  description = "Create the Phase 5 API Gateway integration and NLB ingress rule. This plan-time-known flag must control resource count because the listener ARN and security-group ID are unknown until apply."
  type        = bool
  default     = false
}

variable "serving_target" {
  description = "Which integration the proxy route points at: ecs (default, the Fargate/Cloud Map path) or eks (the Phase 5 front door; requires eks_integration_uri)."
  type        = string
  default     = "ecs"

  validation {
    condition     = contains(["ecs", "eks"], var.serving_target)
    error_message = "serving_target must be one of: ecs, eks."
  }

  validation {
    condition     = var.serving_target != "eks" || var.enable_eks_integration
    error_message = "serving_target = eks requires enable_eks_integration = true."
  }

  validation {
    condition     = var.serving_target != "eks" || var.eks_integration_uri != ""
    error_message = "serving_target = eks requires eks_integration_uri (a Cloud Map service ARN or ELB listener ARN fronting the EKS serving Service)."
  }
}

variable "eks_integration_uri" {
  description = "Integration URI for the EKS serving path: a Cloud Map service ARN or internal NLB listener ARN reachable through the existing VPC Link. Used when enable_eks_integration is true."
  type        = string
  default     = ""
}

variable "eks_nlb_security_group_id" {
  description = "Security group on the Phase 5 internal NLB. Used when enable_eks_integration is true."
  type        = string
  default     = ""
}

# --- WAF ---------------------------------------------------------------------
# WAF has a standing per-web-ACL and per-rule cost, so it is authored but off by
# default. Flip enable_waf to true (and apply) to attach a managed-rule web ACL
# to the stage. Kept minimal: AWS common rule set plus a rate-based rule.

variable "enable_waf" {
  description = "Create and associate a WAFv2 web ACL with the stage (has cost; off by default)."
  type        = bool
  default     = false
}

variable "waf_rate_limit" {
  description = "5-minute request ceiling per source IP for the WAF rate-based rule."
  type        = number
  default     = 2000
}
