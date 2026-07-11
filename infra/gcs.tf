# Raw landing zone: verbatim OpenAQ JSON, partitioned by country/date/sensor
# (gs://{bucket}/raw/openaq/{country}/{YYYY-MM-DD}/{sensor_id}.json).
#
# Versioning supports the immutable-raw principle (G1): an accidental
# overwrite of a raw object is recoverable. force_destroy stays false so a
# `terraform destroy` cannot silently delete landed data.

resource "google_storage_bucket" "raw" {
  name     = local.raw_bucket_name
  location = var.region

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  versioning {
    enabled = true
  }

  depends_on = [google_project_service.apis["storage.googleapis.com"]]
}
