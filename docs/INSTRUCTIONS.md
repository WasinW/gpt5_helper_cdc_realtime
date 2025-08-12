# Instructions

This document explains how to extend the pipeline for new tables:

1. Create YAML mapping for your table (mappings, not_null).
2. Add a `TransformModule` implementing domain logic if needed.
3. Wire it in `CommonRunner.scala` modules list.
4. Provide S3 snapshots under `s3://.../{zone}/{table}/{yyyyMMddHHmm}/` or `mock_s3_dir`.
