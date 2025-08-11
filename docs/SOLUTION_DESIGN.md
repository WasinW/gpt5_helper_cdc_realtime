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
