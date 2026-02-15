# Databricks notebook: silver_merchants
# Purpose: Build silver.merchants (Delta) from bronze bank_a_merchants + bank_b_merchants

from pyspark.sql import functions as F

# -----------------------------
# Config
# -----------------------------
bronze_a_path = f"abfss://bronze@tlmbankdevadls.dfs.core.windows.net/sql/bank_a_merchants.parquet"
bronze_b_path = f"abfss://bronze@tlmbankdevadls.dfs.core.windows.net/sql/bank_b_merchants.parquet"

silver_db_location = f"abfss://silver@tlmbankdevadls.dfs.core.windows.net/hive_metastore/silver.db"
silver_table_location = f"abfss://silver@tlmbankdevadls.dfs.core.windows.net/merchants"

# -----------------------------
# Ensure DB + Table (external Delta in ADLS)
# -----------------------------
spark.sql("USE CATALOG hive_metastore")

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS hive_metastore.silver
LOCATION '{silver_db_location}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.silver.merchants (
  merchant_key STRING,
  merchant_id STRING,
  merchant_name STRING,
  mcc_code STRING,
  country STRING,
  city STRING,
  source_system STRING,
  is_quarantined BOOLEAN
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
df_silver = (
    df.select(
        F.col("merchant_id").cast("string").alias("merchant_id"),
        F.col("merchant_name").cast("string").alias("merchant_name"),
        F.col("mcc_code").cast("string").alias("mcc_code"),
        F.col("country").cast("string").alias("country"),
        F.col("city").cast("string").alias("city"),
        F.col("source_system").cast("string").alias("source_system"),
    )
    .withColumn("merchant_key", F.concat_ws("-", F.col("merchant_id"), F.col("source_system")))
    .withColumn(
        "is_quarantined",
        (F.col("merchant_id").isNull()) | (F.length(F.trim(F.col("merchant_id"))) == 0) |
        (F.col("merchant_name").isNull()) | (F.length(F.trim(F.col("merchant_name"))) == 0) |
        (F.col("mcc_code").isNull()) | (F.length(F.trim(F.col("mcc_code"))) == 0)
    )
    .select("merchant_key", "merchant_id", "merchant_name", "mcc_code", "country", "city", "source_system", "is_quarantined")
    .dropDuplicates(["merchant_key"])
)

df_silver.createOrReplaceTempView("merchants_silver")

# -----------------------------
# Full refresh load
# -----------------------------
spark.sql("TRUNCATE TABLE hive_metastore.silver.merchants")

spark.sql("""
INSERT INTO hive_metastore.silver.merchants
SELECT
  merchant_key, merchant_id, merchant_name, mcc_code, country, city, source_system, is_quarantined
FROM merchants_silver
""")

dbutils.notebook.exit("OK")
