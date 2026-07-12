# Two datasets per the target layout (PROJECT_CONTEXT.md §5):
#   openaq_raw — landing table(s) loaded from GCS (raw JSON, schema-on-read, G1)
#   openaq_dbt — everything dbt materializes (staging/intermediate/mart)
#
# Tables are deliberately NOT defined here: the raw_measurements schema is
# [ASSERTED] until Phase 2/3 land real data, and dbt owns its own relations.
# IaC pinning an asserted schema would couple infrastructure to a guess.

resource "google_bigquery_dataset" "raw" {
  dataset_id  = "openaq_raw"
  location    = var.region
  description = "Raw OpenAQ measurements loaded verbatim from GCS (schema-on-read)."

  depends_on = [google_project_service.apis["bigquery.googleapis.com"]]
}

resource "google_bigquery_dataset" "dbt" {
  dataset_id  = "openaq_dbt"
  location    = var.region
  description = "dbt-managed models: staging, intermediate, and mart layers."

  depends_on = [google_project_service.apis["bigquery.googleapis.com"]]
}
