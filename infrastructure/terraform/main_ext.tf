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
