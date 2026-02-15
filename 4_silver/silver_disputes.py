# Databricks notebook: gold_fact_dispute
# Purpose: Build gold.fact_dispute (Delta) from silver.disputes (current, non-quarantined)
#          + enrich with transaction/account/customer keys

# -----------------------------
# Config
# -----------------------------
gold_db_location = f"abfss://gold@tlmbankdevadls.dfs.core.windows.net/hive_metastore/gold.db"
gold_table_location = f"abfss://gold@tlmbankdevadls.dfs.core.windows.net/fact_dispute"

# -----------------------------
# Ensure DB + Table
# -----------------------------
spark.sql("USE CATALOG hive_metastore")

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS hive_metastore.gold
LOCATION '{gold_db_location}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.gold.fact_dispute (
  dispute_key STRING,
  dispute_id STRING,

  fk_transaction_key STRING,
  transaction_id STRING,

  fk_account_key STRING,
  fk_customer_key STRING,

  dispute_reason STRING,
  dispute_status STRING,
  dispute_amount DOUBLE,
  currency STRING,

  created_at TIMESTAMP,
  resolved_at TIMESTAMP,

  source_system STRING,
  refreshed_at TIMESTAMP
)
USING DELTA
LOCATION '{gold_table_location}'
""")

# -----------------------------
# Full refresh load
# -----------------------------
spark.sql("TRUNCATE TABLE hive_metastore.gold.fact_dispute")

spark.sql("""
INSERT INTO hive_metastore.gold.fact_dispute
SELECT
  d.dispute_key,
  d.dispute_id,

  d.transaction_key        AS fk_transaction_key,
  d.transaction_id,

  t.account_key            AS fk_account_key,
  a.customer_key           AS fk_customer_key,

  d.reason                 AS dispute_reason,
  d.outcome                AS dispute_status,
  CAST(NULL AS DOUBLE)     AS dispute_amount,
  t.currency               AS currency,

  CAST(d.opened_date  AS TIMESTAMP)  AS created_at,
  CAST(d.resolved_date AS TIMESTAMP) AS resolved_at,

  d.source_system,
  current_timestamp() AS refreshed_at
FROM hive_metastore.silver.disputes d
LEFT JOIN hive_metastore.silver.transactions t
  ON d.transaction_key = t.transaction_key
 AND t.is_current = true
 AND t.is_quarantined = false
LEFT JOIN hive_metastore.silver.accounts a
  ON t.account_key = a.account_key
 AND a.is_current = true
 AND a.is_quarantined = false
WHERE d.is_current = true
  AND d.is_quarantined = false
          """)


dbutils.notebook.exit("OK")