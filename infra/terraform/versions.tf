# versions.tf
#
# Provider pinning is mandatory for Harbormaster. War story P8 in
# docs/WAR_STORIES.md describes an anticipated case where a provider minor
# upgrade changes a default and forces a replacement of a stateful resource.
# Every provider is pinned here, so a fresh `terraform init` on any machine
# resolves the same versions.
#
# This file declares the required Terraform CLI version and the providers used
# across the whole project (root and modules). Modules inherit these
# constraints through the configuration; they do not re-declare provider source
# or version (Terraform recommends a single required_providers block per
# configuration for the providers a module passes through).

terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
    helm = {
      # Phase 5 (gate 5.1): KEDA installs via helm_release. 2.x pinned; the
      # 3.x provider changed the kubernetes block to attribute syntax, which
      # would silently break the envs/base provider config on unpinned init.
      source  = "hashicorp/helm"
      version = "~> 2.13"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }
}
