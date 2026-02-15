# Databricks notebook: silver_branches
# Purpose: Build silver.branches (Delta) from bronze bank_a_branches + bank_b_branches

from pyspark.sql import functions as F

# -----------------------------
# Config
# -----------------------------
bronze_a_path = f"abfss://bronze@tlmbankdevadls.dfs.core.windows.net/sql/bank_a_branches.parquet"
bronze_b_path = f"abfss://bronze@tlmbankdevadls.dfs.core.windows.net/sql/bank_b_branches.parquet"

silver_db_location = f"abfss://silver@tlmbankdevadls.dfs.core.windows.net/hive_metastore/silver.db"
silver_table_location = f"abfss://silver@tlmbankdevadls.dfs.core.windows.net/branches"

# -----------------------------
# Ensure DB + Table (external Delta in ADLS)
# -----------------------------
spark.sql("USE CATALOG hive_metastore")

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS hive_metastore.silver
LOCATION '{silver_db_location}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.silver.branches (
  branch_key STRING,
  branch_id STRING,
  branch_name STRING,
  city STRING,
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
        F.col("branch_id").cast("string").alias("branch_id"),
        F.col("branch_name").cast("string").alias("branch_name"),
        F.col("city").cast("string").alias("city"),
        F.col("country").cast("string").alias("country"),
        F.col("source_system").cast("string").alias("source_system"),
    )
    .withColumn("branch_key", F.concat_ws("-", F.col("branch_id"), F.col("source_system")))
    .withColumn(
        "is_quarantined",
        (F.col("branch_id").isNull()) | (F.length(F.trim(F.col("branch_id"))) == 0) |
        (F.col("branch_name").isNull()) | (F.length(F.trim(F.col("branch_name"))) == 0)
    )
    .select("branch_key", "branch_id", "branch_name", "city", "country", "source_system", "is_quarantined")
    .dropDuplicates(["branch_key"])
)

df_silver.createOrReplaceTempView("branches_silver")

# -----------------------------
# Full refresh load
# -----------------------------
spark.sql("TRUNCATE TABLE hive_metastore.silver.branches")

spark.sql("""
INSERT INTO hive_metastore.silver.branches
SELECT
  branch_key, branch_id, branch_name, city, country, source_system, is_quarantined
FROM branches_silver
""")

dbutils.notebook.exit("OK")
