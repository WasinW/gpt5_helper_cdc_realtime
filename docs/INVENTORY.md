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
