# Code Visualization

```
framework/
  core/base/ (PipelineCtx, BaseStage)
  connectors/ (S3JsonlReader, BigQueryDataFetcher)
  pipeline/stages/ (Notification, MessageToRaw, FieldMap, Quality, Transform, Reconcile, BqRead/Write)
  utils/ (YamlLoader, RawConfigLoader, JsonDotPath, SecretManagerUtil)
member-pipeline/
  transformations/ (MemberRefinedTransform, MemberAnalyticsTransform)
  dataflow/ (MemberPipeline)
```
