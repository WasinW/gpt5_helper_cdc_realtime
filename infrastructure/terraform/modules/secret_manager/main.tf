variable "project_id" {}
variable "labels" { type = map(string) }
variable "secrets" { type = map(object({ create = bool })) }
variable "accessor_service_accounts" { type = list(string) }

resource "google_secret_manager_secret" "secrets" {
  for_each = { for k,v in var.secrets : k => v if v.create }
  secret_id  = each.key
  replication { automatic = true }
}

resource "google_secret_manager_secret_iam_member" "access" {
  for_each = { for k,v in var.secrets : k => v if v.create }
  project   = var.project_id
  secret_id = google_secret_manager_secret.secrets[each.key].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${var.accessor_service_accounts[0]}"
}
