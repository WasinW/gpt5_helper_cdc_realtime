# Solution Design – V3 (Common Runner + S3 Reconcile)

- Runner กลาง: `com.analytics.framework.app.CommonRunner` (`--pipeline=raw_ingest|raw_to_structure|refined_ingest|analysis_ingest`, `--config=...`, `--window_id=...`)
- Datasets ต่อโซน: config `datasets.{raw,structure,refined,analytics}`
- Pub/Sub: อ่าน 2 subs (create/update), route table ด้วย `routing_attribute`
- Raw mapping: config-driven (`raw.columns`), รองรับ `attr:`, `json:`, `now_millis`
- Reconcile: ทุกโซน/ทุกตาราง → read S3 JSONL (prefix: `s3://<bucket>/<base_prefix>/<zone>/dt=YYYY-MM-DD/`) → map field GCS↔S3 จาก `reconcile_mapping.yaml`
- AWS creds: `s3.creds.source = gcp_secret_manager|env`; ถ้าใช้ secret manager ให้ตั้ง secret JSON: `{"AWS_ACCESS_KEY_ID":"...","AWS_SECRET_ACCESS_KEY":"...","AWS_REGION":"ap-southeast-1"}`
- Audit logs:
  - `gs://<audit-bucket>/reconcile_log/<domain>/<zone>/<window_id>.log`
  - `gs://<audit-bucket>/data_quality_log/<domain>/<zone>/<window_id>.log`
