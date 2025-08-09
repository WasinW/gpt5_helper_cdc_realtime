variable "project_id" {}
variable "data_bucket" {}
variable "audit_bucket" {}
variable "config_bucket" {}
variable "dlq_bucket" {}
variable "labels" { type = map(string) }
resource "google_storage_bucket" "data" { name=var.data_bucket location="ASIA-SOUTHEAST1" force_destroy=true labels=var.labels uniform_bucket_level_access=true }
resource "google_storage_bucket" "audit" { name=var.audit_bucket location="ASIA-SOUTHEAST1" force_destroy=true labels=var.labels uniform_bucket_level_access=true }
resource "google_storage_bucket" "config" { name=var.config_bucket location="ASIA-SOUTHEAST1" force_destroy=true labels=var.labels uniform_bucket_level_access=true }
resource "google_storage_bucket" "dlq" { name=var.dlq_bucket location="ASIA-SOUTHEAST1" force_destroy=true labels=var.labels uniform_bucket_level_access=true }
output "buckets" { value = { data=google_storage_bucket.data.name, audit=google_storage_bucket.audit.name, config=google_storage_bucket.config.name, dlq=google_storage_bucket.dlq.name } }
