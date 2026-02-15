# Databricks notebook: silver_fx_rates
# Purpose: Build silver.fx_rates (Delta) from bronze api/fx_rates.parquet (folder)

from pyspark.sql import functions as F

# -----------------------------
# Config
# -----------------------------
bronze_path = f"abfss://bronze@tlmbankdevadls.dfs.core.windows.net/api/fx_rates.parquet"

silver_db_location = f"abfss://silver@tlmbankdevadls.dfs.core.windows.net/hive_metastore/silver.db"
silver_table_location = f"abfss://silver@tlmbankdevadls.dfs.core.windows.net/fx_rates"

# -----------------------------
# Ensure DB + Table (external Delta in ADLS)
# -----------------------------
spark.sql("USE CATALOG hive_metastore")

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS hive_metastore.silver
LOCATION '{silver_db_location}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.silver.fx_rates (
  base_currency STRING,
  currency_code STRING,
  currency_name STRING,
  rate DOUBLE,
  obs_date DATE,
  as_of TIMESTAMP,
  is_quarantined BOOLEAN
)
USING DELTA
LOCATION '{silver_table_location}'
""")

# -----------------------------
# Read Bronze (parquet folder)
# -----------------------------
df = spark.read.format("parquet").load(bronze_path)

# -----------------------------
# CDM + DQ
# -----------------------------
df_silver = (
    df.select(
        F.col("base_currency").cast("string").alias("base_currency"),
        F.col("currency_code").cast("string").alias("currency_code"),
        F.col("currency_name").cast("string").alias("currency_name"),
        F.col("rate").cast("double").alias("rate"),
        F.to_date(F.col("obs_date")).alias("obs_date"),
        F.to_timestamp(F.col("as_of")).alias("as_of"),
    )
    .withColumn("base_currency", F.upper(F.trim(F.col("base_currency"))))
    .withColumn("currency_code", F.upper(F.trim(F.col("currency_code"))))
    .withColumn("currency_name", F.trim(F.col("currency_name")))
    .withColumn(
        "is_quarantined",
        (F.col("base_currency").isNull()) | (F.length(F.col("base_currency")) != 3) |
        (F.col("currency_code").isNull()) | (F.length(F.col("currency_code")) != 3) |
        (F.col("rate").isNull()) | (F.col("rate") <= 0) |
        (F.col("obs_date").isNull()) |
        (F.col("as_of").isNull())
    )
    # ensure one row per currency_code for the latest observation date in this load
    .orderBy(F.col("obs_date").desc_nulls_last(), F.col("as_of").desc_nulls_last())
    .dropDuplicates(["base_currency", "currency_code"])
)

df_silver.createOrReplaceTempView("fx_rates_silver")

# -----------------------------
# Full refresh load
# -----------------------------
spark.sql("TRUNCATE TABLE hive_metastore.silver.fx_rates")

spark.sql("""
INSERT INTO hive_metastore.silver.fx_rates
SELECT
  base_currency, currency_code, currency_name, rate, obs_date, as_of, is_quarantined
FROM fx_rates_silver
""")

dbutils.notebook.exit("OK")
