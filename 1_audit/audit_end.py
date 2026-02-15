# Databricks notebook: audit_end
# Purpose: insert a COMPLETED record into audit.audit_logs (Delta)

from datetime import datetime

# ---- Ensure correct catalog ----
spark.sql("USE CATALOG hive_metastore")
spark.sql("USE SCHEMA audit")

# ---- ADF parameters ----
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("pipeline_name", "")
dbutils.widgets.text("layer", "bronze")

dbutils.widgets.text("source_type", "")
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("source_object", "")
dbutils.widgets.text("target_object", "")

dbutils.widgets.text("load_mode", "")
dbutils.widgets.text("watermark_column", "")
dbutils.widgets.text("watermark_value", "")

dbutils.widgets.text("status", "")              # SUCCESS / FAILED
dbutils.widgets.text("rows_processed", "")
dbutils.widgets.text("error_message", "")

# ---- Read params ----
run_id = dbutils.widgets.get("run_id").strip()
pipeline_name = dbutils.widgets.get("pipeline_name").strip()
layer = dbutils.widgets.get("layer").strip()

source_type = dbutils.widgets.get("source_type").strip()
source_system = dbutils.widgets.get("source_system").strip()
source_object = dbutils.widgets.get("source_object").strip()
target_object = dbutils.widgets.get("target_object").strip()

load_mode = dbutils.widgets.get("load_mode").strip()
watermark_column = dbutils.widgets.get("watermark_column").strip()
watermark_value = dbutils.widgets.get("watermark_value").strip()

status = dbutils.widgets.get("status").strip()
rows_processed = dbutils.widgets.get("rows_processed").strip()
error_message = dbutils.widgets.get("error_message").strip()

# ---- Guardrails ----
if not run_id or not pipeline_name or not layer or not source_type or not source_object or not target_object or not status:
    raise ValueError(
        "Missing required parameters for audit_end"
    )

# ---- Safe SQL formatting ----
def to_sql_nullable_string(value: str):
    if value == "":
        return "NULL"
    safe = value.replace("'", "''")
    return "'" + safe + "'"

source_system_sql = to_sql_nullable_string(source_system)
load_mode_sql = to_sql_nullable_string(load_mode)
watermark_column_sql = to_sql_nullable_string(watermark_column)
watermark_value_sql = to_sql_nullable_string(watermark_value)
error_message_sql = to_sql_nullable_string(error_message)

# rows_processed (numeric or NULL)
rows_sql = "NULL" if rows_processed == "" else rows_processed

safe_status = status.replace("'", "''")
status_sql = "'" + safe_status + "'"

# ---- Append-only INSERT ----
insert_sql = f"""
INSERT INTO hive_metastore.audit.audit_logs (
  run_id, pipeline_name, layer,
  source_type, source_system, source_object, target_object,
  load_mode, watermark_column, watermark_value,
  rows_processed, status, error_message,
  start_time, end_time, run_date, inserted_at
)
VALUES (
  '{run_id.replace("'", "''")}',
  '{pipeline_name.replace("'", "''")}',
  '{layer.replace("'", "''")}',
  '{source_type.replace("'", "''")}',
  {source_system_sql},
  '{source_object.replace("'", "''")}',
  '{target_object.replace("'", "''")}',
  {load_mode_sql},
  {watermark_column_sql},
  {watermark_value_sql},
  {rows_sql},
  {status_sql},
  {error_message_sql},
  NULL,
  current_timestamp(),
  current_date(),
  current_timestamp()
)
"""

spark.sql(insert_sql)

dbutils.notebook.exit("OK")