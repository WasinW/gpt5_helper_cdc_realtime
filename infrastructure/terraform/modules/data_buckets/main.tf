variable "buckets" { type = map(string) }
variable "labels" { type = map(string) }
resource "google_storage_bucket" "b" {
  for_each  = var.buckets
  name      = each.value
  location  = "ASIA-SOUTHEAST1"
  force_destroy = true
  uniform_bucket_level_access = true
  labels = var.labels
}
output "names" { value = { for k, v in google_storage_bucket.b : k => v.name } }
