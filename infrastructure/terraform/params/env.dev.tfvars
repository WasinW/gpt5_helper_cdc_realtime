project_id   = "demo-the-1"
region       = "asia-southeast1"
domain       = "member"
data_bucket  = "demo-the-1-staging"
audit_bucket = "demo-the-1-audit"
config_bucket= "demo-the-1-config"
dlq_bucket   = "demo-the-1-dlq"
labels = { environment="dev", service="cdc", domain="member", team="data-platform", owner="team@example.com", cost_center="data-analytics", managed_by="terraform" }
