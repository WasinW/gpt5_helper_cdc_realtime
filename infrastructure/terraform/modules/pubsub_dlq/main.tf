variable "project_id" {}
variable "labels" { type = map(string) }
variable "create_topic" {}
variable "update_topic" {}
variable "create_sub" {}
variable "update_sub" {}
variable "create_dlq_topic" {}
variable "update_dlq_topic" {}
variable "create_dlq_sub" {}
variable "update_dlq_sub" {}

resource "google_pubsub_topic" "create_dlq" { name = var.create_dlq_topic  labels = var.labels }
resource "google_pubsub_topic" "update_dlq" { name = var.update_dlq_topic  labels = var.labels }

resource "google_pubsub_subscription" "create_sub" {
  name  = var.create_sub
  topic = var.create_topic
  labels = var.labels
  ack_deadline_seconds = 20
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.create_dlq.id
    max_delivery_attempts = 10
  }
}

resource "google_pubsub_subscription" "update_sub" {
  name  = var.update_sub
  topic = var.update_topic
  labels = var.labels
  ack_deadline_seconds = 20
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.update_dlq.id
    max_delivery_attempts = 10
  }
}

resource "google_pubsub_subscription" "create_dlq_sub" {
  name  = var.create_dlq_sub
  topic = google_pubsub_topic.create_dlq.name
  labels = var.labels
  ack_deadline_seconds = 20
}

resource "google_pubsub_subscription" "update_dlq_sub" {
  name  = var.update_dlq_sub
  topic = google_pubsub_topic.update_dlq.name
  labels = var.labels
  ack_deadline_seconds = 20
}
