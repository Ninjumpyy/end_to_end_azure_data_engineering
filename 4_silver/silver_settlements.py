# Databricks notebook: silver_settlements_append
# Purpose: Append-only load into silver.settlements from bronze flat files:

from pyspark.sql import functions as F

# -----------------------------
# Config
# -----------------------------
bronze_a_path = f"abfss://bronze@tlmbankdevadls.dfs.core.windows.net/flat_files/bank_a_settlements.parquet"
bronze_b_path = f"abfss://bronze@tlmbankdevadls.dfs.core.windows.net/flat_files/bank_b_settlements.parquet"

silver_db_location = f"abfss://silver@tlmbankdevadls.dfs.core.windows.net/hive_metastore/silver.db"
silver_table_location = f"abfss://silver@tlmbankdevadls.dfs.core.windows.net/settlements"

# -----------------------------
# Ensure DB + Table (external Delta in ADLS)
# -----------------------------
spark.sql("USE CATALOG hive_metastore")

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS hive_metastore.silver
LOCATION '{silver_db_location}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.silver.settlements (
  settlement_key STRING,
  settlement_id STRING,
  transaction_key STRING,
  transaction_id STRING,
  settlement_date DATE,
  settled_amount DOUBLE,
  currency STRING,
  fx_rate_used DOUBLE,
  fees DOUBLE,
  settlement_status STRING,
  source_system STRING,
  is_quarantined BOOLEAN,
  audit_insertdate TIMESTAMP
)
USING DELTA
LOCATION '{silver_table_location}'
""")

# -----------------------------
# Read Bronze + add source_system
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
# CDM + DQ + keys
# -----------------------------
df_silver = (
    df.select(
        F.col("settlement_id").cast("string").alias("settlement_id"),
        F.col("transaction_id").cast("string").alias("transaction_id"),
        F.to_date(F.col("settlement_date")).alias("settlement_date"),
        F.col("settled_amount").cast("double").alias("settled_amount"),
        F.upper(F.col("currency").cast("string")).alias("currency"),
        F.col("fx_rate_used").cast("double").alias("fx_rate_used"),
        F.col("fees").cast("double").alias("fees"),
        F.upper(F.col("settlement_status").cast("string")).alias("settlement_status"),
        F.col("source_system").cast("string").alias("source_system"),
    )
    .withColumn("settlement_key", F.concat_ws("-", F.col("settlement_id"), F.col("source_system")))
    .withColumn("transaction_key", F.concat_ws("-", F.col("transaction_id"), F.col("source_system")))
    .withColumn(
        "is_quarantined",
        (F.col("settlement_id").isNull()) | (F.length(F.trim(F.col("settlement_id"))) == 0) |
        (F.col("transaction_id").isNull()) | (F.length(F.trim(F.col("transaction_id"))) == 0) |
        (F.col("settlement_date").isNull()) |
        (F.col("settled_amount").isNull()) |
        (F.col("currency").isNull()) | (F.length(F.trim(F.col("currency"))) != 3)
    )
    .withColumn("audit_insertdate", F.current_timestamp())
)

df_silver.createOrReplaceTempView("settlements_stg")

# -----------------------------
# Append-only with de-dupe on settlement_key
# (prevents re-inserting same settlement if the file is reprocessed)
# -----------------------------
spark.sql("""
INSERT INTO hive_metastore.silver.settlements
SELECT
  s.settlement_key,
  s.settlement_id,
  s.transaction_key,
  s.transaction_id,
  s.settlement_date,
  s.settled_amount,
  s.currency,
  s.fx_rate_used,
  s.fees,
  s.settlement_status,
  s.source_system,
  s.is_quarantined,
  s.audit_insertdate
FROM settlements_stg s
LEFT ANTI JOIN hive_metastore.silver.settlements t
  ON t.settlement_key = s.settlement_key
""")

dbutils.notebook.exit("OK")
