-- Framework tables for audit & dq & reconcile (create per domain e.g. member_framework)
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DOMAIN}_framework.pipeline_log` (
  log_id STRING NOT NULL,
  pipeline_name STRING NOT NULL,
  domain STRING NOT NULL,
  zone STRING NOT NULL,
  table_name STRING NOT NULL,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  status STRING NOT NULL,
  metrics JSON,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
CLUSTER BY domain, zone, table_name;

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DOMAIN}_framework.reconcile_log` (
  reconcile_id STRING NOT NULL,
  domain STRING NOT NULL,
  zone STRING NOT NULL,
  table_name STRING NOT NULL,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  gcp_count INT64,
  aws_count INT64,
  count_match BOOL,
  mismatch_samples ARRAY<JSON>,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
CLUSTER BY domain, zone, table_name;

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DOMAIN}_framework.data_quality_log` (
  quality_id STRING NOT NULL,
  domain STRING NOT NULL,
  zone STRING NOT NULL,
  table_name STRING NOT NULL,
  rule_name STRING NOT NULL,
  passed BOOL,
  details JSON,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
CLUSTER BY domain, zone, table_name;