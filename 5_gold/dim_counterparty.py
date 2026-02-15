# Databricks notebook: gold_dim_counterparty
# Purpose: Build gold.dim_counterparty (Delta) from silver.counterparties (non-quarantined)

# -----------------------------
# Config
# -----------------------------
gold_db_location = f"abfss://gold@tlmbankdevadls.dfs.core.windows.net/hive_metastore/gold.db"
gold_table_location = f"abfss://gold@tlmbankdevadls.dfs.core.windows.net/dim_counterparty"

# -----------------------------
# Ensure DB + Table (external Delta in ADLS)
# -----------------------------
spark.sql("USE CATALOG hive_metastore")

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS hive_metastore.gold
LOCATION '{gold_db_location}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.gold.dim_counterparty (
  counterparty_key STRING,
  counterparty_id STRING,
  counterparty_name STRING,
  counterparty_type STRING,
  bank_bic STRING,
  country STRING,
  source_system STRING,
  refreshed_at TIMESTAMP
)
USING DELTA
LOCATION '{gold_table_location}'
""")

# -----------------------------
# Full refresh load from Silver (non-quarantined)
# -----------------------------
spark.sql("TRUNCATE TABLE hive_metastore.gold.dim_counterparty")

spark.sql("""
INSERT INTO hive_metastore.gold.dim_counterparty
SELECT
  counterparty_key,
  counterparty_id,
  counterparty_name,
  counterparty_type,
  bank_bic,
  country,
  source_system,
  current_timestamp() AS refreshed_at
FROM hive_metastore.silver.counterparties
WHERE is_quarantined = false
""")

dbutils.notebook.exit("OK")
