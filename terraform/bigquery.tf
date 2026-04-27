# Raw dataset — mirrors GCS raw zone, minimal transformation
resource "google_bigquery_dataset" "raw" {
  dataset_id  = "raw"
  description = "Raw ingested data — source of truth, never modified"
  location    = var.region

  labels = {
    environment = var.environment
    layer       = "raw"
  }
}

# Staging dataset — cleaned, typed, deduplicated
resource "google_bigquery_dataset" "staging" {
  dataset_id  = "staging"
  description = "Cleaned and validated data, ready for transformation"
  location    = var.region

  labels = {
    environment = var.environment
    layer       = "staging"
  }
}

# Marts dataset — business-ready star schema for BI tools
resource "google_bigquery_dataset" "marts" {
  dataset_id  = "marts"
  description = "Business-ready dimensional models for dashboards"
  location    = var.region

  labels = {
    environment = var.environment
    layer       = "marts"
  }
}