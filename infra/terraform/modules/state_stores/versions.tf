# modules/state_stores/versions.tf
#
# Version constraints for this module. Pins mirror envs/base and the war-story
# P8 policy in infra/terraform/versions.tf; declared per module so tflint's
# terraform_required_version / terraform_required_providers checks pass at
# warning severity.

terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}
