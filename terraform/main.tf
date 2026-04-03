terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ── GCS Bucket ────────────────────────────────────────────────────────────────
resource "google_storage_bucket" "raw" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

# ── BigQuery Datasets ─────────────────────────────────────────────────────────
resource "google_bigquery_dataset" "raw" {
  dataset_id = "olist_raw"
  location   = var.region
  description = "Raw data loaded directly from GCS"
}

resource "google_bigquery_dataset" "staging" {
  dataset_id  = "olist_staging"
  location    = var.region
  description = "Cleaned and typed data"
}

resource "google_bigquery_dataset" "marts" {
  dataset_id  = "olist_marts"
  location    = var.region
  description = "Business-level models"
}