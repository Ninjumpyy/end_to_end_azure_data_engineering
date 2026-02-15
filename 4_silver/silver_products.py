# Databricks notebook: silver_products
# Purpose: Build silver.products (Delta) from bronze bank_a_products + bank_b_products
# - Same product catalog exists in both banks -> we de-duplicate across banks in Silver.

from pyspark.sql import functions as F

# -----------------------------
# Config
# -----------------------------
bronze_a_path = f"abfss://bronze@tlmbankdevadls.dfs.core.windows.net/sql/bank_a_products.parquet"
bronze_b_path = f"abfss://bronze@tlmbankdevadls.dfs.core.windows.net/sql/bank_b_products.parquet"

silver_db_location = f"abfss://silver@tlmbankdevadls.dfs.core.windows.net/hive_metastore/silver.db"
silver_table_location = f"abfss://silver@tlmbankdevadls.dfs.core.windows.net/products"

# -----------------------------
# Ensure DB + Table (external Delta in ADLS)
# -----------------------------
spark.sql("USE CATALOG hive_metastore")

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS hive_metastore.silver
LOCATION '{silver_db_location}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.silver.products (
  product_id STRING,
  product_type STRING,
  currency STRING,
  interest_rate DOUBLE,
  monthly_fee DOUBLE,
  is_quarantined BOOLEAN
)
USING DELTA
LOCATION '{silver_table_location}'
""")

# -----------------------------
# Read Bronze (Parquet) + union
# -----------------------------
df_a = spark.read.format("parquet").load(bronze_a_path)
df_b = spark.read.format("parquet").load(bronze_b_path)

df = df_a.unionByName(df_b, allowMissingColumns=True)

# -----------------------------
# CDM + DQ
# -----------------------------
df_silver = (
    df.select(
        F.col("product_id").cast("string").alias("product_id"),
        F.col("product_type").cast("string").alias("product_type"),
        F.col("currency").cast("string").alias("currency"),
        F.col("interest_rate").cast("double").alias("interest_rate"),
        F.col("monthly_fee").cast("double").alias("monthly_fee"),
    )
    .withColumn(
        "is_quarantined",
        (F.col("product_id").isNull()) | (F.length(F.trim(F.col("product_id"))) == 0) |
        (F.col("product_type").isNull()) | (F.length(F.trim(F.col("product_type"))) == 0) |
        (F.col("currency").isNull()) | (F.length(F.trim(F.col("currency"))) == 0) |
        (F.col("interest_rate").isNull()) | (F.col("interest_rate") < 0) |
        (F.col("monthly_fee").isNull()) | (F.col("monthly_fee") < 0)
    )
    # since both banks have the same product catalog, keep one row per product_id
    .dropDuplicates(["product_id"])
)

df_silver.createOrReplaceTempView("products_silver")

# -----------------------------
# Full refresh load
# -----------------------------
spark.sql("TRUNCATE TABLE hive_metastore.silver.products")

spark.sql("""
INSERT INTO hive_metastore.silver.products
SELECT
  product_id, product_type, currency, interest_rate, monthly_fee, is_quarantined
FROM products_silver
""")

dbutils.notebook.exit("OK")
