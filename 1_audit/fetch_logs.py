# Databricks notebook: /Shared/01_audit/fetch_logs
# Purpose: return the latest watermark_value (string) for a given (source_system, source_object)
# Returns: dbutils.notebook.exit("<watermark_value>") or "" if none

from pyspark.sql import functions as F

# --- Widgets / parameters ---
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("source_object", "")

source_system    = dbutils.widgets.get("source_system").strip()
source_object    = dbutils.widgets.get("source_object").strip()

if not source_system or not source_object:
    raise ValueError("source_system and source_object are required")

# --- Load audit delta ---
df = spark.table("audit.audit_logs")

# --- Basic filters ---
last = (
    df.filter(
        (F.col("source_system") == source_system) &
        (F.col("source_object") == source_object) &
        (F.col("status") == "SUCCESS") &
        (F.col("watermark_value").isNotNull() &
         (F.length(F.col("watermark_value")) > 0))
    )
    .orderBy(F.col("end_time").desc_nulls_last(), F.col("inserted_at").desc_nulls_last())
    .select("watermark_value")
    .limit(1)
    .collect()
)

watermark_value = last[0]["watermark_value"] if last else ""
dbutils.notebook.exit(watermark_value)