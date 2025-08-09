variable "project_id" { type = string }
variable "region"     { type = string  default = "asia-southeast1" }
variable "domain"     { type = string  default = "member" }
variable "data_bucket"   { type = string  default = "demo-the-1-staging" }
variable "audit_bucket"  { type = string  default = "demo-the-1-audit" }
variable "config_bucket" { type = string  default = "demo-the-1-config" }
variable "dlq_bucket"    { type = string  default = "demo-the-1-dlq" }
variable "labels" { type = map(string) default = { environment="dev", service="cdc", domain="member", team="data-platform", owner="owner@example.com", cost_center="data-analytics", managed_by="terraform" } }
