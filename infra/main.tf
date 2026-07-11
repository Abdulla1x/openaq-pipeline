# Provider requirements and remote state backend.
#
# The backend bucket is created once, outside Terraform (see README.md):
# the bucket that stores the state cannot be provisioned by that same state.
# Backend blocks cannot interpolate variables, so the bucket name is literal.

terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }

  backend "gcs" {
    bucket = "REPLACE_WITH_PROJECT_ID-tfstate" # set after the GCP project exists
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
