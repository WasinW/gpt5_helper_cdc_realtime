terraform {
  required_version = ">= 1.6.0"
  required_providers {
    google = { source = "hashicorp/google", version = "~> 5.32" }
  }
  backend "gcs" { bucket = "demo-the-1-tf-state", prefix = "cdc/" }
}
provider "google" { project = var.project_id, region = var.region }
locals { labels = var.labels }
module "buckets" { source="./modules/buckets" project_id=var.project_id data_bucket=var.data_bucket audit_bucket=var.audit_bucket config_bucket=var.config_bucket dlq_bucket=var.dlq_bucket labels=local.labels }
module "pubsub"  { source="./modules/pubsub"  project_id=var.project_id domain=var.domain create_topic="member-events-create" update_topic="member-events-update" create_sub="member-events-create-sub" update_sub="member-events-update-sub" create_dlq_sub="member-events-create-dlq-sub" update_dlq_sub="member-events-update-dlq-sub" labels=local.labels }
module "bigquery"{ source="./modules/bigquery" project_id=var.project_id domain=var.domain labels=local.labels }
module "monitoring"{ source="./modules/monitoring" project_id=var.project_id domain=var.domain audit_bucket=var.audit_bucket labels=local.labels }
