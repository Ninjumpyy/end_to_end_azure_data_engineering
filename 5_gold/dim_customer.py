# Databricks notebook: gold_dim_customer
# Purpose: Build gold.dim_customer (Delta) from silver.customers (current, non-quarantined)

# -----------------------------
# Config
# -----------------------------
gold_db_location = f"abfss://gold@tlmbankdevadls.dfs.core.windows.net/hive_metastore/gold.db"
gold_table_location = f"abfss://gold@tlmbankdevadls.dfs.core.windows.net/dim_customer"

# -----------------------------
# Ensure DB + Table (external Delta in ADLS)
# -----------------------------
spark.sql("USE CATALOG hive_metastore")

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS hive_metastore.gold
LOCATION '{gold_db_location}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.gold.dim_customer (
  customer_key STRING,
  customer_id STRING,
  first_name STRING,
  last_name STRING,
  dob DATE,
  country STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  source_system STRING,
  refreshed_at TIMESTAMP
)
USING DELTA
LOCATION '{gold_table_location}'
""")

# -----------------------------
# Full refresh load from Silver (current + non-quarantined)
# -----------------------------
spark.sql("TRUNCATE TABLE hive_metastore.gold.dim_customer")

spark.sql("""
INSERT INTO hive_metastore.gold.dim_customer
SELECT
  customer_key,
  customer_id,
  first_name,
  last_name,
  dob,
  country,
  created_at,
  updated_at,
  source_system,
  current_timestamp() AS refreshed_at
FROM hive_metastore.silver.customers
WHERE is_quarantined = false
  AND is_current = true
""")

dbutils.notebook.exit("OK")
