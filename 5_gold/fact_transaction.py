# Databricks notebook: gold_fact_transaction
# Purpose: Build gold.fact_transaction (Delta) from silver.transaction (current, non-quarantined)
#          + enrich with customer_key via silver.accounts

# -----------------------------
# Config
# -----------------------------
gold_db_location = f"abfss://gold@tlmbankdevadls.dfs.core.windows.net/hive_metastore/gold.db"
gold_table_location = f"abfss://gold@tlmbankdevadls.dfs.core.windows.net/fact_transaction"

# -----------------------------
# Ensure DB + Table (external Delta in ADLS)
# -----------------------------
spark.sql("USE CATALOG hive_metastore")

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS hive_metastore.gold
LOCATION '{gold_db_location}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.gold.fact_transaction (
  transaction_key STRING,
  transaction_id STRING,

  fk_account_key STRING,
  account_id STRING,

  fk_customer_key STRING,

  fk_merchant_key STRING,
  merchant_id STRING,

  fk_counterparty_key STRING,
  counterparty_id STRING,

  booking_ts TIMESTAMP,
  value_ts TIMESTAMP,
  amount DOUBLE,
  currency STRING,
  direction STRING,
  channel STRING,
  txn_type STRING,
  status STRING,

  source_system STRING,
  refreshed_at TIMESTAMP
)
USING DELTA
LOCATION '{gold_table_location}'
""")

# -----------------------------
# Full refresh load (current + non-quarantined)
# -----------------------------
spark.sql("TRUNCATE TABLE hive_metastore.gold.fact_transaction")

spark.sql("""
INSERT INTO hive_metastore.gold.fact_transaction
SELECT
  t.transaction_key,
  t.transaction_id,

  t.account_key       AS fk_account_key,
  t.account_id,

  a.customer_key      AS fk_customer_key,

  t.merchant_key      AS fk_merchant_key,
  t.merchant_id,

  t.counterparty_key  AS fk_counterparty_key,
  t.counterparty_id,

  t.booking_ts,
  t.value_ts,
  t.amount,
  t.currency,
  t.direction,
  t.channel,
  t.txn_type,
  t.status,

  t.source_system,
  current_timestamp() AS refreshed_at
FROM hive_metastore.silver.transactions t
LEFT JOIN hive_metastore.silver.accounts a
  ON t.account_key = a.account_key
 AND a.is_current = true
 AND a.is_quarantined = false
WHERE t.is_current = true
  AND t.is_quarantined = false
""")

dbutils.notebook.exit("OK")
