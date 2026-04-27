# Raw landing zone — data lands here exactly as ingested, never modified
resource "google_storage_bucket" "raw" {
  name          = "${var.project_id}-raw"
  location      = var.region
  force_destroy = false

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  labels = {
    environment = var.environment
    layer       = "raw"
    project     = "urban-intelligence"
  }
}

# Staging zone — cleaned, validated data before BigQuery load
resource "google_storage_bucket" "staging" {
  name          = "${var.project_id}-staging"
  location      = var.region
  force_destroy = false

  versioning {
    enabled = true
  }

  labels = {
    environment = var.environment
    layer       = "staging"
    project     = "urban-intelligence"
  }
}

# Scripts bucket — PySpark jobs, Python scripts uploaded here
resource "google_storage_bucket" "scripts" {
  name          = "${var.project_id}-scripts"
  location      = var.region
  force_destroy = false

  labels = {
    environment = var.environment
    layer       = "scripts"
    project     = "urban-intelligence"
  }
}