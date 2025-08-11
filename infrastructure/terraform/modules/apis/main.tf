variable "project_id" {}
variable "services" { type = list(string) }
resource "google_project_service" "svc" {
  for_each           = toset(var.services)
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}
