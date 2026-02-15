# Databricks notebook: silver_transactions_scd2
# Purpose: Maintain silver.transactions as SCD Type 2 from bronze incremental drops:

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -----------------------------
# Config
# -----------------------------
bronze_a_glob = f"abfss://bronze@tlmbankdevadls.dfs.core.windows.net/sql/incr/bank_a_transactions_*.parquet"
bronze_b_glob = f"abfss://bronze@tlmbankdevadls.dfs.core.windows.net/sql/incr/bank_b_transactions_*.parquet"

silver_db_location = f"abfss://silver@tlmbankdevadls.dfs.core.windows.net/hive_metastore/silver.db"
silver_table_location = f"abfss://silver@tlmbankdevadls.dfs.core.windows.net/transactions"

# -----------------------------
# Ensure DB + Table (external Delta in ADLS)
# -----------------------------
spark.sql("USE CATALOG hive_metastore")

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS hive_metastore.silver
LOCATION '{silver_db_location}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.silver.transactions (
  transaction_key STRING,
  transaction_id STRING,
  account_key STRING,
  account_id STRING,
  booking_ts TIMESTAMP,
  value_ts TIMESTAMP,
  amount DOUBLE,
  currency STRING,
  direction STRING,
  channel STRING,
  merchant_key STRING,
  merchant_id STRING,
  counterparty_key STRING,
  counterparty_id STRING,
  txn_type STRING,
  status STRING,
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
# Read Bronze incremental drops (glob) + add source_system
# -----------------------------
df_a = (
    spark.read.format("parquet").load(bronze_a_glob)
    .withColumn("source_system", F.lit("bank_a"))
)

df_b = (
    spark.read.format("parquet").load(bronze_b_glob)
    .withColumn("source_system", F.lit("bank_b"))
)

df = df_a.unionByName(df_b, allowMissingColumns=True)

# -----------------------------
# CDM + DQ + keep latest per transaction_key within the batch
# -----------------------------
stg_raw = (
    df.select(
        F.col("transaction_id").cast("string").alias("transaction_id"),
        F.col("account_id").cast("string").alias("account_id"),
        F.to_timestamp(F.col("booking_ts")).alias("booking_ts"),
        F.to_timestamp(F.col("value_ts")).alias("value_ts"),
        F.col("amount").cast("double").alias("amount"),
        F.upper(F.col("currency").cast("string")).alias("currency"),
        F.upper(F.col("direction").cast("string")).alias("direction"),
        F.col("channel").cast("string").alias("channel"),
        F.col("merchant_id").cast("string").alias("merchant_id"),
        F.col("counterparty_id").cast("string").alias("counterparty_id"),
        F.col("txn_type").cast("string").alias("txn_type"),
        F.upper(F.col("status").cast("string")).alias("status"),
        F.col("source_system").cast("string").alias("source_system"),
    )
    .withColumn("transaction_key", F.concat_ws("-", F.col("transaction_id"), F.col("source_system")))
    .withColumn("account_key", F.concat_ws("-", F.col("account_id"), F.col("source_system")))
    .withColumn("merchant_key", F.when(F.col("merchant_id").isNull(), F.lit(None))
                .otherwise(F.concat_ws("-", F.col("merchant_id"), F.col("source_system"))))
    .withColumn("counterparty_key", F.when(F.col("counterparty_id").isNull(), F.lit(None))
                .otherwise(F.concat_ws("-", F.col("counterparty_id"), F.col("source_system"))))
    .withColumn(
        "is_quarantined",
        (F.col("transaction_id").isNull()) | (F.length(F.trim(F.col("transaction_id"))) == 0) |
        (F.col("account_id").isNull()) | (F.length(F.trim(F.col("account_id"))) == 0) |
        (F.col("booking_ts").isNull()) |
        (F.col("amount").isNull()) |
        (F.col("currency").isNull()) | (F.length(F.trim(F.col("currency"))) != 3) |
        (~F.col("direction").isin("DEBIT", "CREDIT"))
    )
)

# if multiple files contain the same transaction, keep the latest by booking_ts/value_ts
w = Window.partitionBy("transaction_key").orderBy(
    F.col("booking_ts").desc_nulls_last(),
    F.col("value_ts").desc_nulls_last()
)

stg = (
    stg_raw
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

stg.createOrReplaceTempView("transactions_stg")

# -----------------------------
# SCD2 step 1: expire current rows if something changed
# -----------------------------
spark.sql("""
MERGE INTO hive_metastore.silver.transactions AS target
USING transactions_stg AS source
ON target.transaction_key = source.transaction_key AND target.is_current = true
WHEN MATCHED AND (
     target.account_key         <> source.account_key
  OR target.account_id          <> source.account_id
  OR target.booking_ts          <> source.booking_ts
  OR target.value_ts            <> source.value_ts
  OR target.amount              <> source.amount
  OR target.currency            <> source.currency
  OR target.direction           <> source.direction
  OR target.channel             <> source.channel
  OR target.merchant_key        <> source.merchant_key
  OR target.merchant_id         <> source.merchant_id
  OR target.counterparty_key    <> source.counterparty_key
  OR target.counterparty_id     <> source.counterparty_id
  OR target.txn_type            <> source.txn_type
  OR target.status              <> source.status
  OR target.source_system       <> source.source_system
  OR target.is_quarantined      <> source.is_quarantined
) THEN UPDATE SET
  target.is_current = false,
  target.audit_modifieddate = current_timestamp()
""")

# -----------------------------
# SCD2 step 2: insert new current rows (new + changed)
# -----------------------------
spark.sql("""
MERGE INTO hive_metastore.silver.transactions AS target
USING transactions_stg AS source
ON target.transaction_key = source.transaction_key AND target.is_current = true
WHEN NOT MATCHED THEN INSERT (
  transaction_key,
  transaction_id,
  account_key,
  account_id,
  booking_ts,
  value_ts,
  amount,
  currency,
  direction,
  channel,
  merchant_key,
  merchant_id,
  counterparty_key,
  counterparty_id,
  txn_type,
  status,
  source_system,
  is_quarantined,
  audit_insertdate,
  audit_modifieddate,
  is_current
) VALUES (
  source.transaction_key,
  source.transaction_id,
  source.account_key,
  source.account_id,
  source.booking_ts,
  source.value_ts,
  source.amount,
  source.currency,
  source.direction,
  source.channel,
  source.merchant_key,
  source.merchant_id,
  source.counterparty_key,
  source.counterparty_id,
  source.txn_type,
  source.status,
  source.source_system,
  source.is_quarantined,
  current_timestamp(),
  current_timestamp(),
  true
)
""")

dbutils.notebook.exit("OK")
