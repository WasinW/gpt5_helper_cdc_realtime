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
