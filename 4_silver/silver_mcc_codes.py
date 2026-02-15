# Databricks notebook: silver_mcc_codes
# Purpose: Build silver.mcc_codes (Delta) from bronze flat_files/mcc_codes.parquet

from pyspark.sql import functions as F

# -----------------------------
# Config
# -----------------------------
bronze_path = f"abfss://bronze@tlmbankdevadls.dfs.core.windows.net/flat_files/mcc_codes.parquet"

silver_db_location = f"abfss://silver@tlmbankdevadls.dfs.core.windows.net/hive_metastore/silver.db"
silver_table_location = f"abfss://silver@tlmbankdevadls.dfs.core.windows.net/mcc_codes"

# -----------------------------
# Ensure DB + Table (external Delta in ADLS)
# -----------------------------
spark.sql("USE CATALOG hive_metastore")

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS hive_metastore.silver
LOCATION '{silver_db_location}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.silver.mcc_codes (
  mcc_code STRING,
  mcc_description STRING,
  mcc_category STRING,
  is_quarantined BOOLEAN
)
USING DELTA
LOCATION '{silver_table_location}'
""")

# -----------------------------
# Read Bronze (Parquet)
# -----------------------------
df = spark.read.format("parquet").load(bronze_path)

# -----------------------------
# CDM + DQ
# -----------------------------
df_silver = (
    df.select(
        F.col("mcc_code").cast("string").alias("mcc_code"),
        F.col("mcc_description").cast("string").alias("mcc_description"),
        F.col("mcc_category").cast("string").alias("mcc_category"),
    )
    .withColumn("mcc_code", F.trim(F.col("mcc_code")))
    .withColumn("mcc_description", F.trim(F.col("mcc_description")))
    .withColumn("mcc_category", F.trim(F.col("mcc_category")))
    .withColumn(
        "is_quarantined",
        (F.col("mcc_code").isNull()) | (F.length(F.col("mcc_code")) == 0) |
        (F.col("mcc_description").isNull()) | (F.length(F.col("mcc_description")) == 0)
    )
    .dropDuplicates(["mcc_code"])
)

df_silver.createOrReplaceTempView("mcc_codes_silver")

# -----------------------------
# Full refresh load
# -----------------------------
spark.sql("TRUNCATE TABLE hive_metastore.silver.mcc_codes")

spark.sql("""
INSERT INTO hive_metastore.silver.mcc_codes
SELECT
  mcc_code, mcc_description, mcc_category, is_quarantined
FROM mcc_codes_silver
""")

dbutils.notebook.exit("OK")
