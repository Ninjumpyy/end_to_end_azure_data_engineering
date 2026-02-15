# Databricks notebook: audit_start
# Purpose: insert a STARTED record into audit.audit_logs (Delta)

from datetime import datetime

# ---- Ensure correct catalog ----
spark.sql("USE CATALOG hive_metastore")
spark.sql("USE SCHEMA audit")

# ---- ADF parameters (passed as base parameters) ----
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

# ---- Guardrails ----
if not run_id or not pipeline_name or not layer or not source_type or not source_object or not target_object:
    raise ValueError(
        "Missing required parameters. Required: run_id, pipeline_name, layer, source_type, source_object, target_object"
    )

if watermark_column == "":
    watermark_column_sql = "NULL"
else:
    safe_wm_col = watermark_column.replace("'", "''")
    watermark_column_sql = f"'{safe_wm_col}'"

# watermark_value
if watermark_value == "":
    watermark_value_sql = "NULL"
else:
    safe_wm_val = watermark_value.replace("'", "''")
    watermark_value_sql = f"'{safe_wm_val}'"

# source_system
if source_system == "":
    source_system_sql = "NULL"
else:
    safe_source = source_system.replace("'", "''")
    source_system_sql = f"'{safe_source}'"

# load_mode
if load_mode == "":
    load_mode_sql = "NULL"
else:
    safe_load = load_mode.replace("'", "''")
    load_mode_sql = f"'{safe_load}'"

# watermark_column_sql = "NULL" if watermark_column == "" else f"'{watermark_column.replace(\"'\", \"''\")}'"
# watermark_value_sql = "NULL" if watermark_value == "" else f"'{watermark_value.replace(\"'\", \"''\")}'"
# source_system_sql = "NULL" if source_system == "" else f"'{source_system.replace(\"'\", \"''\")}'"
# load_mode_sql = "NULL" if load_mode == "" else f"'{load_mode.replace(\"'\", \"''\")}'"

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
  NULL,
  'STARTED',
  NULL,
  current_timestamp(),
  NULL,
  current_date(),
  current_timestamp()
)
"""

spark.sql(insert_sql)

dbutils.notebook.exit("OK")