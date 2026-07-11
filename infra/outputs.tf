output "raw_bucket_name" {
  description = "GCS bucket receiving raw OpenAQ JSON (GCS_BUCKET_NAME in .env)."
  value       = google_storage_bucket.raw.name
}

output "raw_dataset_id" {
  description = "BigQuery dataset for raw loaded data."
  value       = google_bigquery_dataset.raw.dataset_id
}

output "dbt_dataset_id" {
  description = "BigQuery dataset for dbt models."
  value       = google_bigquery_dataset.dbt.dataset_id
}

output "service_account_email" {
  description = "Pipeline service account (key created via gcloud, see README)."
  value       = google_service_account.pipeline.email
}
