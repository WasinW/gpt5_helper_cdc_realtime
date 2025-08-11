variable "project_id" {}
variable "region" {}
variable "labels" { type = map(string) }
variable "lake_name" {}
variable "buckets" { type = object({ raw = string, analytics = string }) }

resource "google_dataplex_lake" "lake" {
  name     = var.lake_name
  location = var.region
  project  = var.project_id
  labels   = var.labels
}

resource "google_dataplex_zone" "raw_zone" {
  lake     = google_dataplex_lake.lake.name
  location = var.region
  project  = var.project_id
  name     = "raw-zone"
  type     = "RAW"
  discovery_spec { enabled = true }
}

resource "google_dataplex_zone" "analytics_zone" {
  lake     = google_dataplex_lake.lake.name
  location = var.region
  project  = var.project_id
  name     = "analytics-zone"
  type     = "CURATED"
  discovery_spec { enabled = true }
}

resource "google_dataplex_asset" "raw_bucket" {
  name     = "raw-bucket"
  lake     = google_dataplex_lake.lake.name
  location = var.region
  project  = var.project_id
  zone     = google_dataplex_zone.raw_zone.name
  discovery_spec { enabled = true }
  resource_spec {
    name = "projects/${var.project_id}/buckets/${var.buckets.raw}"
    type = "STORAGE_BUCKET"
  }
}

resource "google_dataplex_asset" "analytics_bucket" {
  name     = "analytics-bucket"
  lake     = google_dataplex_lake.lake.name
  location = var.region
  project  = var.project_id
  zone     = google_dataplex_zone.analytics_zone.name
  discovery_spec { enabled = true }
  resource_spec {
    name = "projects/${var.project_id}/buckets/${var.buckets.analytics}"
    type = "STORAGE_BUCKET"
  }
}
