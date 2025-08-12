# gpt5_helper_cdc_realtime

This repo contains a minimal Apache Beam (Scala) framework and a sample **member** pipeline that ingests Pub/Sub notifications, fetches records, performs **quality checks**, **transforms**, and **reconciles** against S3 snapshots, then writes to BigQuery.

## Projects
- `framework/` – reusable stages, connectors, and utilities
- `member-pipeline/` – example domain pipeline wiring

See `docs/ARCHITECTURE.md` and `docs/DEVELOPMENT.md`.
