# Enable the GCP APIs the pipeline depends on. A fresh project has most APIs
# disabled; creating a bucket or dataset before its API is enabled fails.
# disable_on_destroy = false: destroying infra should not switch APIs off for
# the whole project (other resources or consoles may still rely on them).

resource "google_project_service" "apis" {
  for_each = toset([
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "iam.googleapis.com",
  ])

  service            = each.key
  disable_on_destroy = false
}
