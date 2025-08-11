#!/usr/bin/env bash
set -euo pipefail

BRANCH="feature/cdc-refactor-full"
git rev-parse --is-inside-work-tree >/dev/null 2>&1 || { echo "❌ Not a git repo"; exit 1; }
git fetch origin || true
git checkout "$BRANCH"

mkdir -p infrastructure/terraform/modules/{apis,iam,secret_manager,network,data_buckets,pubsub_dlq,dataplex,biglake} \
         infrastructure/terraform/params docs

# ---------------- variables (extended) ----------------
cat > infrastructure/terraform/variables_ext.tf <<'EOF'
variable "project_tag" { type = string  default = "new_data_plateform" }

variable "framework_bucket"         { type = string  default = "dp_framework" }
variable "data_secure_bucket"       { type = string  default = "dp_data_secure" }
variable "data_analytics_bucket"    { type = string  default = "dp_data_analytics" }
variable "data_secure_dlq_bucket"   { type = string  default = "dp_data_secure_dlq" }
variable "data_analytics_dlq_bucket"{ type = string  default = "dp_data_analytics_dlq" }
EOF

# ---------------- enable core APIs ----------------
cat > infrastructure/terraform/modules/apis/main.tf <<'EOF'
variable "project_id" {}
variable "services" { type = list(string) }
resource "google_project_service" "svc" {
  for_each           = toset(var.services)
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}
EOF

# ---------------- IAM / SA ----------------
cat > infrastructure/terraform/modules/iam/main.tf <<'EOF'
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
EOF

# ---------------- Secret Manager ----------------
cat > infrastructure/terraform/modules/secret_manager/main.tf <<'EOF'
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
EOF

# ---------------- Network (VPC/Subnet/NAT) ----------------
cat > infrastructure/terraform/modules/network/main.tf <<'EOF'
variable "project_id" {}
variable "region" {}
variable "labels" { type = map(string) }
variable "network_name" { default = "cdc-vpc" }
variable "subnetwork_name" { default = "cdc-subnet" }

resource "google_compute_network" "vpc" {
  name                    = var.network_name
  project                 = var.project_id
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = "${var.subnetwork_name}-${var.region}"
  ip_cidr_range = "10.10.0.0/20"
  region        = var.region
  project       = var.project_id
  network       = google_compute_network.vpc.id
  private_ip_google_access = true
}

resource "google_compute_router" "router" {
  name    = "cdc-router"
  region  = var.region
  network = google_compute_network.vpc.name
}

resource "google_compute_router_nat" "nat" {
  name                               = "cdc-nat"
  router                             = google_compute_router.router.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "LIST_OF_SUBNETWORKS"
  subnetwork {
    name                    = google_compute_subnetwork.subnet.id
    source_ip_ranges_to_nat = ["ALL_IP_RANGES"]
  }
}

output "subnetwork_self_link" { value = google_compute_subnetwork.subnet.self_link }
EOF

# ---------------- Data buckets (framework/data/dlq) ----------------
cat > infrastructure/terraform/modules/data_buckets/main.tf <<'EOF'
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
EOF

# ---------------- Pub/Sub DLQ (separate topics) ----------------
cat > infrastructure/terraform/modules/pubsub_dlq/main.tf <<'EOF'
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
EOF

# ---------------- Dataplex ----------------
cat > infrastructure/terraform/modules/dataplex/main.tf <<'EOF'
variable "project_id" {}
variable "region" {}
variable "labels" { type = map(string) }
variable "lake_name" {}
variable "buckets" { type = object({ raw = string, analytics = string }) }

resource "google_dataplex_lake" "lake" {
  name     = var.lake_name
  location = var.region
  project  = var.project_id
  labels   = var.labels
}

resource "google_dataplex_zone" "raw_zone" {
  lake     = google_dataplex_lake.lake.name
  location = var.region
  project  = var.project_id
  name     = "raw-zone"
  type     = "RAW"
  discovery_spec { enabled = true }
}

resource "google_dataplex_zone" "analytics_zone" {
  lake     = google_dataplex_lake.lake.name
  location = var.region
  project  = var.project_id
  name     = "analytics-zone"
  type     = "CURATED"
  discovery_spec { enabled = true }
}

resource "google_dataplex_asset" "raw_bucket" {
  name     = "raw-bucket"
  lake     = google_dataplex_lake.lake.name
  location = var.region
  project  = var.project_id
  zone     = google_dataplex_zone.raw_zone.name
  discovery_spec { enabled = true }
  resource_spec {
    name = "projects/${var.project_id}/buckets/${var.buckets.raw}"
    type = "STORAGE_BUCKET"
  }
}

resource "google_dataplex_asset" "analytics_bucket" {
  name     = "analytics-bucket"
  lake     = google_dataplex_lake.lake.name
  location = var.region
  project  = var.project_id
  zone     = google_dataplex_zone.analytics_zone.name
  discovery_spec { enabled = true }
  resource_spec {
    name = "projects/${var.project_id}/buckets/${var.buckets.analytics}"
    type = "STORAGE_BUCKET"
  }
}
EOF

# ---------------- BigLake (external parquet) ----------------
cat > infrastructure/terraform/modules/biglake/main.tf <<'EOF'
variable "project_id" {}
variable "dataset_id" {}
variable "table_id" {}
variable "gcs_uris" { type = list(string) }
variable "labels" { type = map(string) }

resource "google_bigquery_table" "external_parquet" {
  project    = var.project_id
  dataset_id = var.dataset_id
  table_id   = var.table_id
  labels     = var.labels

  external_data_configuration {
    source_format = "PARQUET"
    autodetect    = true
    source_uris   = var.gcs_uris
    hive_partitioning_options {
      mode = "AUTO"
      require_partition_filter = false
    }
  }
}
EOF

# ---------------- main (extended) ----------------
cat > infrastructure/terraform/main_ext.tf <<'EOF'
module "apis" {
  source     = "./modules/apis"
  project_id = var.project_id
  services = [
    "compute.googleapis.com",
    "pubsub.googleapis.com",
    "bigquery.googleapis.com",
    "bigqueryconnection.googleapis.com",
    "dataplex.googleapis.com",
    "dataflow.googleapis.com",
    "secretmanager.googleapis.com",
    "storage.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "iam.googleapis.com"
  ]
}

module "iam" {
  source     = "./modules/iam"
  project_id = var.project_id
  labels     = merge(var.labels, { project = var.project_tag })
}

module "secrets" {
  source     = "./modules/secret_manager"
  project_id = var.project_id
  labels     = merge(var.labels, { project = var.project_tag })
  secrets = { member_api_token = { create = true } }
  accessor_service_accounts = [ module.iam.sa_runner_email ]
}

module "network" {
  source     = "./modules/network"
  project_id = var.project_id
  region     = var.region
  labels     = merge(var.labels, { project = var.project_tag })
  network_name   = "cdc-vpc"
  subnetwork_name= "cdc-subnet"
}

module "data_buckets" {
  source = "./modules/data_buckets"
  labels = merge(var.labels, { project = var.project_tag })
  buckets = {
    framework          = var.framework_bucket
    data_secure        = var.data_secure_bucket
    data_analytics     = var.data_analytics_bucket
    data_secure_dlq    = var.data_secure_dlq_bucket
    data_analytics_dlq = var.data_analytics_dlq_bucket
  }
}

module "pubsub_dlq_fix" {
  source             = "./modules/pubsub_dlq"
  project_id         = var.project_id
  labels             = merge(var.labels, { project = var.project_tag })
  create_topic       = "member-events-create"
  update_topic       = "member-events-update"
  create_sub         = "member-events-create-sub"
  update_sub         = "member-events-update-sub"
  create_dlq_topic   = "member-events-create-dlq"
  update_dlq_topic   = "member-events-update-dlq"
  create_dlq_sub     = "member-events-create-dlq-sub"
  update_dlq_sub     = "member-events-update-dlq-sub"
}

module "dataplex" {
  source     = "./modules/dataplex"
  project_id = var.project_id
  region     = var.region
  labels     = merge(var.labels, { project = var.project_tag })
  lake_name  = "analytics-lake"
  buckets = {
    raw       = var.data_secure_bucket
    analytics = var.data_analytics_bucket
  }
}

module "biglake_example" {
  source        = "./modules/biglake"
  project_id    = var.project_id
  dataset_id    = "member_analytics"
  table_id      = "member_parquet_ext"
  gcs_uris      = ["gs://${var.data_analytics_bucket}/member/analytics/sbl/*/*.parquet"]
  labels        = merge(var.labels, { project = var.project_tag })
}
EOF

# ---------------- tfvars (extended) ----------------
cat > infrastructure/terraform/params/env.dev.ext.tfvars <<'EOF'
project_tag = "new_data_plateform"
framework_bucket          = "dp_framework"
data_secure_bucket        = "dp_data_secure"
data_analytics_bucket     = "dp_data_analytics"
data_secure_dlq_bucket    = "dp_data_secure_dlq"
data_analytics_dlq_bucket = "dp_data_analytics_dlq"
EOF

# ---------------- docs ----------------
cat > docs/SOLUTION_DESIGN.md <<'EOF'
# Solution Design (Updated)

- Global label `project = new_data_plateform` (param `project_tag`).
- VPC/Subnet/NAT (Private Google Access) for Dataflow and private egress.
- Service Account: `svc-cdc-dataflow@<project>.iam.gserviceaccount.com`
  - roles: dataflow.admin, dataflow.worker, pubsub.admin, storage.admin, bigquery.admin, secretmanager.secretAccessor, dataplex.viewer
- Secrets: `member_api_token` in Secret Manager; SA has accessor.
- Pub/Sub DLQ with dedicated topics: `member-events-create-dlq`, `member-events-update-dlq` (+ subs)
- GCS layout added:
  - `gs://dp_framework/...` (framework/pipeline scripts & resources)
  - `gs://dp_data_secure/...` + `gs://dp_data_secure_dlq/...`
  - `gs://dp_data_analytics/...` + `gs://dp_data_analytics_dlq/...`
- Dataplex: lake `analytics-lake`, zones `raw-zone` & `analytics-zone`, assets pointing to buckets.
- BigLake: external Parquet table `member_analytics.member_parquet_ext` over GCS.
EOF

cat > docs/INVENTORY.md <<'EOF'
# Inventory – Services & Infra (latest)

## GCS
- demo-the-1-staging, demo-the-1-audit, demo-the-1-config, demo-the-1-dlq
- dp_framework
- dp_data_secure, dp_data_secure_dlq
- dp_data_analytics, dp_data_analytics_dlq

## Pub/Sub
- Topics: member-events-create, member-events-update, member-events-create-dlq, member-events-update-dlq
- Subs: member-events-create-sub, member-events-update-sub, member-events-create-dlq-sub, member-events-update-dlq-sub

## BigQuery
- Datasets: member_raw, member_structure, member_refined, member_analytics
- BigLake external table: member_analytics.member_parquet_ext (PARQUET on GCS)

## Dataflow
- Runner SA: svc-cdc-dataflow@<project>
- Subnetwork: cdc-subnet-<region> (via VPC module)

## Secret Manager
- member_api_token (value to be added manually)

## Dataplex
- Lake: analytics-lake; Zones: raw-zone, analytics-zone; Assets: GCS buckets (raw/analytics)

## IAM
- SA roles: dataflow.admin, dataflow.worker, pubsub.admin, storage.admin, bigquery.admin, secretmanager.secretAccessor, dataplex.viewer

## Labels/Tags
- All resources include `project = new_data_plateform`
EOF

git add -A
git commit -m "infra(expand): APIs enable, SA/IAM, Secret Manager, VPC+NAT, Pub/Sub DLQ topics, extra GCS buckets, Dataplex, BigLake; project tag param"
git push
echo "✅ Pushed infra expansion. Next: terraform init/apply with ext tfvars."
