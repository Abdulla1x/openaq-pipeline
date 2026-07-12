# Pipeline service account with least-privilege, resource-level grants.
# No project-level Editor/Owner: the SA can touch exactly the raw bucket and
# the two datasets. bigquery.jobUser is the one project-scoped grant because
# query/load jobs are project-level objects in BigQuery.
#
# The SA's key is created with `gcloud iam service-accounts keys create`, not
# a google_service_account_key resource — Terraform-managed keys store the
# private key in plaintext in the tfstate (against the spirit of G10).

resource "google_service_account" "pipeline" {
  account_id   = "openaq-pipeline"
  display_name = "OpenAQ pipeline"
  description  = "Used by Airflow ingestion and dbt to write GCS raw data and manage BigQuery tables."

  depends_on = [google_project_service.apis["iam.googleapis.com"]]
}

resource "google_storage_bucket_iam_member" "raw_object_admin" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "raw_data_editor" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "dbt_data_editor" {
  dataset_id = google_bigquery_dataset.dbt.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}
