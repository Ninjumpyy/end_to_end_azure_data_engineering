# Databricks notebook: silver_accounts
# Purpose: Build/maintain silver.accounts as SCD Type 2 Delta table from bronze bank_a_accounts + bank_b_accounts

from pyspark.sql import functions as F

# -----------------------------
# Config
# -----------------------------
bronze_a_path = f"abfss://bronze@tlmbankdevadls.dfs.core.windows.net/sql/bank_a_accounts.parquet"
bronze_b_path = f"abfss://bronze@tlmbankdevadls.dfs.core.windows.net/sql/bank_b_accounts.parquet"

silver_db_location = f"abfss://silver@tlmbankdevadls.dfs.core.windows.net/hive_metastore/silver.db"
silver_table_location = f"abfss://silver@tlmbankdevadls.dfs.core.windows.net/accounts"

# -----------------------------
# Ensure DB + Table (external Delta in ADLS)
# -----------------------------
spark.sql("USE CATALOG hive_metastore")

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS hive_metastore.silver
LOCATION '{silver_db_location}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.silver.accounts (
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
  is_quarantined BOOLEAN,
  audit_insertdate TIMESTAMP,
  audit_modifieddate TIMESTAMP,
  is_current BOOLEAN
)
USING DELTA
LOCATION '{silver_table_location}'
""")

# -----------------------------
# Read Bronze (Parquet) + add source_system
# -----------------------------
df_a = (
    spark.read.format("parquet").load(bronze_a_path)
    .withColumn("source_system", F.lit("bank_a"))
)

df_b = (
    spark.read.format("parquet").load(bronze_b_path)
    .withColumn("source_system", F.lit("bank_b"))
)

df = df_a.unionByName(df_b, allowMissingColumns=True)

# -----------------------------
# CDM + DQ
# -----------------------------
stg = (
    df.select(
        F.col("account_id").cast("string").alias("account_id"),
        F.col("customer_id").cast("string").alias("customer_id"),
        F.col("iban").cast("string").alias("iban"),
        F.col("product_id").cast("string").alias("product_id"),
        F.col("branch_id").cast("string").alias("branch_id"),
        F.col("currency").cast("string").alias("currency"),
        F.upper(F.col("status").cast("string")).alias("status"),
        F.to_timestamp(F.col("opened_at")).alias("opened_at"),
        F.to_timestamp(F.col("closed_at")).alias("closed_at"),
        F.col("source_system").cast("string").alias("source_system"),
    )
    .withColumn("account_key", F.concat_ws("-", F.col("account_id"), F.col("source_system")))
    .withColumn("customer_key", F.concat_ws("-", F.col("customer_id"), F.col("source_system")))
    .withColumn(
        "is_quarantined",
        (F.col("account_id").isNull()) | (F.length(F.trim(F.col("account_id"))) == 0) |
        (F.col("customer_id").isNull()) | (F.length(F.trim(F.col("customer_id"))) == 0) |
        (F.col("product_id").isNull()) | (F.length(F.trim(F.col("product_id"))) == 0) |
        (F.col("branch_id").isNull()) | (F.length(F.trim(F.col("branch_id"))) == 0) |
        (F.col("currency").isNull()) | (F.length(F.trim(F.col("currency"))) == 0) |
        # invalid date range if both present
        (F.col("opened_at").isNotNull() & F.col("closed_at").isNotNull() & (F.col("closed_at") < F.col("opened_at")))
    )
    .select(
        "account_key", "account_id",
        "customer_key", "customer_id",
        "iban", "product_id", "branch_id", "currency", "status",
        "opened_at", "closed_at",
        "source_system", "is_quarantined"
    )
    .dropDuplicates(["account_key"])
)

stg.createOrReplaceTempView("accounts_stg")

# -----------------------------
# SCD2 step 1: expire current rows if something changed
# -----------------------------
spark.sql("""
MERGE INTO hive_metastore.silver.accounts AS target
USING accounts_stg AS source
ON target.account_key = source.account_key AND target.is_current = true
WHEN MATCHED AND (
     target.customer_key      <> source.customer_key
  OR target.customer_id       <> source.customer_id
  OR target.iban              <> source.iban
  OR target.product_id        <> source.product_id
  OR target.branch_id         <> source.branch_id
  OR target.currency          <> source.currency
  OR target.status            <> source.status
  OR target.opened_at         <> source.opened_at
  OR target.closed_at         <> source.closed_at
  OR target.source_system     <> source.source_system
  OR target.is_quarantined    <> source.is_quarantined
) THEN UPDATE SET
  target.is_current = false,
  target.audit_modifieddate = current_timestamp()
""")

# -----------------------------
# SCD2 step 2: insert new current rows (new accounts + changed accounts)
# -----------------------------
spark.sql("""
MERGE INTO hive_metastore.silver.accounts AS target
USING accounts_stg AS source
ON target.account_key = source.account_key AND target.is_current = true
WHEN NOT MATCHED THEN INSERT (
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
  is_quarantined,
  audit_insertdate,
  audit_modifieddate,
  is_current
) VALUES (
  source.account_key,
  source.account_id,
  source.customer_key,
  source.customer_id,
  source.iban,
  source.product_id,
  source.branch_id,
  source.currency,
  source.status,
  source.opened_at,
  source.closed_at,
  source.source_system,
  source.is_quarantined,
  current_timestamp(),
  current_timestamp(),
  true
)
""")

dbutils.notebook.exit("OK")