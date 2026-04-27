# Dedicated service account for the entire pipeline
resource "google_service_account" "pipeline_sa" {
  account_id   = "urban-pipeline-sa"
  display_name = "Urban Intelligence Pipeline Service Account"
  description  = "Used by Dataproc, Airflow, and ingestion scripts"
}

# Least-privilege roles — only what the pipeline actually needs
locals {
  pipeline_roles = [
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin",
    "roles/dataproc.worker",
    "roles/pubsub.editor",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
  ]
}

resource "google_project_iam_member" "pipeline_sa_roles" {
  for_each = toset(local.pipeline_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

output "pipeline_sa_email" {
  value       = google_service_account.pipeline_sa.email
  description = "Service account email — use this in Python scripts"
}