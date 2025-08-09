variable "project_id" {}
variable "domain" {}
variable "create_topic" {}
variable "update_topic" {}
variable "create_sub" {}
variable "update_sub" {}
variable "create_dlq_sub" {}
variable "update_dlq_sub" {}
variable "labels" { type = map(string) }
resource "google_pubsub_topic" "create" { name=var.create_topic labels=var.labels }
resource "google_pubsub_topic" "update" { name=var.update_topic labels=var.labels }
resource "google_pubsub_subscription" "create" { name=var.create_sub topic=google_pubsub_topic.create.name labels=var.labels message_retention_duration="604800s" ack_deadline_seconds=20 dead_letter_policy {{ dead_letter_topic = google_pubsub_topic.create.name max_delivery_attempts = 10 }} }
resource "google_pubsub_subscription" "update" { name=var.update_sub topic=google_pubsub_topic.update.name labels=var.labels message_retention_duration="604800s" ack_deadline_seconds=20 dead_letter_policy {{ dead_letter_topic = google_pubsub_topic.update.name max_delivery_attempts = 10 }} }
