# Architecture

- **NotificationStage**: converts Pub/Sub messages to an internal record.
- **MessageToRawStage**: parses incoming JSON message to a `Map[String,Any]`.
- **QualityStage**: not-null checks based on YAML rules.
- **TransformStage**: invokes domain `TransformModule` to produce structure/refined/analytics shapes.
- **ReconcileStage**: compares GCP records vs S3 JSONL snapshot per table/zone and logs mismatches.
- **BqWriteDynamicStage**: writes to BigQuery (templated table destination).

## Key classes
- `PipelineCtx`: ambient config (projectId, region, windowId, buckets...).
- `TransformModule`: SPI for user-defined transforms.
- `YamlLoader` / `RawConfigLoader`: YAML helpers to load config/rules.
- `JsonDotPath`: extract with dot-path from JSON strings.
- `S3JsonlReader`: read `.jsonl` files from S3 (or local mock) into Maps.
