# Databricks notebook: gold_dim_merchant
# Purpose: Build gold.dim_merchant (Delta) from silver.merchants (non-quarantined)

# -----------------------------
# Config
# -----------------------------
gold_db_location = f"abfss://gold@tlmbankdevadls.dfs.core.windows.net/hive_metastore/gold.db"
gold_table_location = f"abfss://gold@tlmbankdevadls.dfs.core.windows.net/dim_merchant"

# -----------------------------
# Ensure DB + Table (external Delta in ADLS)
# -----------------------------
spark.sql("USE CATALOG hive_metastore")

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS hive_metastore.gold
LOCATION '{gold_db_location}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.gold.dim_merchant (
  merchant_key STRING,
  merchant_id STRING,
  merchant_name STRING,
  mcc_code STRING,
  country STRING,
  city STRING,
  source_system STRING,
  refreshed_at TIMESTAMP
)
USING DELTA
LOCATION '{gold_table_location}'
""")

# -----------------------------
# Full refresh load from Silver (non-quarantined)
# -----------------------------
spark.sql("TRUNCATE TABLE hive_metastore.gold.dim_merchant")

spark.sql("""
INSERT INTO hive_metastore.gold.dim_merchant
SELECT
  merchant_key,
  merchant_id,
  merchant_name,
  mcc_code,
  country,
  city,
  source_system,
  current_timestamp() AS refreshed_at
FROM hive_metastore.silver.merchants
WHERE is_quarantined = false
""")

dbutils.notebook.exit("OK")
