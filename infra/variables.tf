variable "project_id" {
  description = "GCP project ID (globally unique)."
  type        = string
}

variable "region" {
  description = "Region for the GCS bucket and BigQuery datasets. US regions qualify for the GCS always-free tier; bucket and datasets are colocated so GCS-to-BQ load jobs work without cross-region copies."
  type        = string
  default     = "us-central1"
}

locals {
  # Bucket names are globally unique; prefixing with the project ID avoids collisions.
  raw_bucket_name = "${var.project_id}-openaq-raw"
}
