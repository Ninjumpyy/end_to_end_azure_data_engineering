CREATE DATABASE IF NOT EXISTS hive_metastore.audit
LOCATION 'abfss://audit@tlmbankdevadls.dfs.core.windows.net/hive_metastore/audit.db';

CREATE TABLE IF NOT EXISTS hive_metastore.audit.audit_logs (
  run_id STRING,                 -- ADF pipeline run id
  pipeline_name STRING,          -- ingestion pipeline name
  layer STRING,                  -- bronze / silver / gold

  source_type STRING,            -- sql | landing | api
  source_system STRING,          -- bank_a, bank_b, external, api_name
  source_object STRING,          -- table name or file path
  target_object STRING,          -- bronze path/table

  load_mode STRING,              -- full | incremental | snapshot
  watermark_column STRING,       -- e.g. updated_at, file_month
  watermark_value STRING,        -- last processed value

  rows_processed BIGINT,
  status STRING,                 -- STARTED | SUCCESS | FAILED
  error_message STRING,

  start_time TIMESTAMP,
  end_time TIMESTAMP,
  run_date DATE,                 -- logical run date
  inserted_at TIMESTAMP          -- audit insert time
)
USING DELTA
LOCATION 'abfss://audit@tlmbankdevadls.dfs.core.windows.net/audit_logs';

