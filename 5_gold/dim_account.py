# Databricks notebook: gold_dim_account
# Purpose: Build gold.dim_account (Delta) from silver.accounts (current, non-quarantined)

# -----------------------------
# Config
# -----------------------------
gold_db_location = f"abfss://gold@tlmbankdevadls.dfs.core.windows.net/hive_metastore/gold.db"
gold_table_location = f"abfss://gold@tlmbankdevadls.dfs.core.windows.net/dim_account"

# -----------------------------
# Ensure DB + Table (external Delta in ADLS)
# -----------------------------
spark.sql("USE CATALOG hive_metastore")

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS hive_metastore.gold
LOCATION '{gold_db_location}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.gold.dim_account (
  account_key STRING,
  account_id STRING,
  customer_key STRING,
  customer_id STRING,
  iban STRING,
  product_id STRING,
  branch_id STRING,
  currency STRING,
  status STRING,
  opened_at TIMESTAMP,
  closed_at TIMESTAMP,
  source_system STRING,
  refreshed_at TIMESTAMP
)
USING DELTA
LOCATION '{gold_table_location}'
""")

# -----------------------------
# Full refresh load from Silver (current + non-quarantined)
# -----------------------------
spark.sql("TRUNCATE TABLE hive_metastore.gold.dim_account")

spark.sql("""
INSERT INTO hive_metastore.gold.dim_account
SELECT
  account_key,
  account_id,
  customer_key,
  customer_id,
  iban,
  product_id,
  branch_id,
  currency,
  status,
  opened_at,
  closed_at,
  source_system,
  current_timestamp() AS refreshed_at
FROM hive_metastore.silver.accounts
WHERE is_quarantined = false
  AND is_current = true
""")

dbutils.notebook.exit("OK")
