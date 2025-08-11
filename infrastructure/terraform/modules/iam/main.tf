variable "project_id" {}
variable "labels" { type = map(string) }
resource "google_service_account" "runner" {
  account_id   = "svc-cdc-dataflow"
  display_name = "CDC Dataflow Runner"
  project      = var.project_id
}
resource "google_project_iam_member" "runner_roles" {
  for_each = toset([
    "roles/dataflow.admin",
    "roles/dataflow.worker",
    "roles/pubsub.admin",
    "roles/storage.admin",
    "roles/bigquery.admin",
    "roles/secretmanager.secretAccessor",
    "roles/dataplex.viewer",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.runner.email}"
}
output "sa_runner_email" { value = google_service_account.runner.email }
