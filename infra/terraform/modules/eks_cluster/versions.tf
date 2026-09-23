# modules/eks_cluster/versions.tf
#
# Version constraints for this module. Pins mirror envs/base and the war-story
# P8 policy in infra/terraform/versions.tf; declared per module so tflint's
# terraform_required_version / terraform_required_providers checks pass at
# warning severity. helm is pinned to the 2.x line: the 3.x provider changed
# the kubernetes block to attribute syntax, which would silently break the
# envs/base provider configuration on an unpinned init.

terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.13"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }
}
