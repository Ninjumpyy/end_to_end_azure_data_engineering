# Databricks notebook: gold_fact_settlement
# Purpose: Build gold.fact_settlement (Delta) from silver.settlements (non-quarantined)
#          + enrich with transaction/account/customer keys

# -----------------------------
# Config
# -----------------------------
gold_db_location = f"abfss://gold@tlmbankdevadls.dfs.core.windows.net/hive_metastore/gold.db"
gold_table_location = f"abfss://gold@tlmbankdevadls.dfs.core.windows.net/fact_settlement"

# -----------------------------
# Ensure DB + Table
# -----------------------------
spark.sql("USE CATALOG hive_metastore")

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS hive_metastore.gold
LOCATION '{gold_db_location}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.gold.fact_settlement (
  settlement_key STRING,
  settlement_id STRING,

  fk_transaction_key STRING,
  transaction_id STRING,

  fk_account_key STRING,
  fk_customer_key STRING,

  settlement_date DATE,
  settled_amount DOUBLE,
  currency STRING,
  fx_rate_used DOUBLE,
  fees DOUBLE,
  settlement_status STRING,

  source_system STRING,
  refreshed_at TIMESTAMP
)
USING DELTA
LOCATION '{gold_table_location}'
""")

# -----------------------------
# Full refresh load
# -----------------------------
spark.sql("TRUNCATE TABLE hive_metastore.gold.fact_settlement")

spark.sql("""
INSERT INTO hive_metastore.gold.fact_settlement
SELECT
  s.settlement_key,
  s.settlement_id,

  s.transaction_key      AS fk_transaction_key,
  s.transaction_id,

  t.account_key          AS fk_account_key,
  a.customer_key         AS fk_customer_key,

  s.settlement_date,
  s.settled_amount,
  s.currency,
  s.fx_rate_used,
  s.fees,
  s.settlement_status,

  s.source_system,
  current_timestamp() AS refreshed_at
FROM hive_metastore.silver.settlements s
LEFT JOIN hive_metastore.silver.transactions t
  ON s.transaction_key = t.transaction_key
 AND t.is_current = true
 AND t.is_quarantined = false
LEFT JOIN hive_metastore.silver.accounts a
  ON t.account_key = a.account_key
 AND a.is_current = true
 AND a.is_quarantined = false
WHERE s.is_quarantined = false
""")

dbutils.notebook.exit("OK")
