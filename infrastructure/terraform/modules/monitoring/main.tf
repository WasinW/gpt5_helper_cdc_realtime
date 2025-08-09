variable "project_id" {}
variable "domain" {}
variable "audit_bucket" {}
variable "labels" { type = map(string) }
output "monitoring_note" { value = "Monitoring resources will be added in subsequent patches." }
