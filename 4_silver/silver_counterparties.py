# Databricks notebook: silver_counterparties
# Purpose: Build silver.counterparties (Delta) from bronze bank_a_counterparties + bank_b_counterparties

from pyspark.sql import functions as F

# -----------------------------
# Config
# -----------------------------
bronze_a_path = f"abfss://bronze@tlmbankdevadls.dfs.core.windows.net/sql/bank_a_counterparties.parquet"
bronze_b_path = f"abfss://bronze@tlmbankdevadls.dfs.core.windows.net/sql/bank_b_counterparties.parquet"

silver_db_location = f"abfss://silver@tlmbankdevadls.dfs.core.windows.net/hive_metastore/silver.db"
silver_table_location = f"abfss://silver@tlmbankdevadls.dfs.core.windows.net/counterparties"

# -----------------------------
# Ensure DB + Table (external Delta in ADLS)
# -----------------------------
spark.sql("USE CATALOG hive_metastore")

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS hive_metastore.silver
LOCATION '{silver_db_location}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.silver.counterparties (
  counterparty_key STRING,
  counterparty_id STRING,
  counterparty_name STRING,
  counterparty_type STRING,
  bank_bic STRING,
  country STRING,
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
        F.col("counterparty_id").cast("string").alias("counterparty_id"),
        F.col("counterparty_name").cast("string").alias("counterparty_name"),
        F.col("counterparty_type").cast("string").alias("counterparty_type"),
        F.col("bank_bic").cast("string").alias("bank_bic"),
        F.col("country").cast("string").alias("country"),
        F.col("source_system").cast("string").alias("source_system"),
    )
    .withColumn("counterparty_key", F.concat_ws("-", F.col("counterparty_id"), F.col("source_system")))
    .withColumn(
        "is_quarantined",
        (F.col("counterparty_id").isNull()) | (F.length(F.trim(F.col("counterparty_id"))) == 0) |
        (F.col("counterparty_name").isNull()) | (F.length(F.trim(F.col("counterparty_name"))) == 0) |
        (F.col("counterparty_type").isNull()) | (F.length(F.trim(F.col("counterparty_type"))) == 0)
    )
    .select(
        "counterparty_key", "counterparty_id", "counterparty_name",
        "counterparty_type", "bank_bic", "country", "source_system", "is_quarantined"
    )
    .dropDuplicates(["counterparty_key"])
)

df_silver.createOrReplaceTempView("counterparties_silver")

# -----------------------------
# Full refresh load
# -----------------------------
spark.sql("TRUNCATE TABLE hive_metastore.silver.counterparties")

spark.sql("""
INSERT INTO hive_metastore.silver.counterparties
SELECT
  counterparty_key, counterparty_id, counterparty_name, counterparty_type, bank_bic, country, source_system, is_quarantined
FROM counterparties_silver
""")

dbutils.notebook.exit("OK")