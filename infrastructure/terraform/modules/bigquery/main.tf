variable "project_id" {}
variable "domain" {}
variable "labels" { type = map(string) }
resource "google_bigquery_dataset" "raw" { dataset_id="${var.domain}_raw" location="asia-southeast1" delete_contents_on_destroy=true labels=var.labels }
resource "google_bigquery_dataset" "structure" { dataset_id="${var.domain}_structure" location="asia-southeast1" delete_contents_on_destroy=true labels=var.labels }
resource "google_bigquery_dataset" "refined" { dataset_id="${var.domain}_refined" location="asia-southeast1" delete_contents_on_destroy=true labels=var.labels }
resource "google_bigquery_dataset" "analytics" { dataset_id="${var.domain}_analytics" location="asia-southeast1" delete_contents_on_destroy=true labels=var.labels }
