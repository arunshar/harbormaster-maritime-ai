# modules/eks_teardown_guard/versions.tf
#
# Version constraints for this module. Pins mirror envs/base and the war-story
# P8 policy in infra/terraform/versions.tf; declared per module so tflint's
# terraform_required_version / terraform_required_providers checks pass at
# warning severity.

terraform {
  required_version = ">= 1.9"

  required_providers {
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}
