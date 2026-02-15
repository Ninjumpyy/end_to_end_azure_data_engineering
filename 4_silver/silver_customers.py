# Databricks notebook: silver_customers
# Purpose: Maintain silver.customers as SCD Type 2 from bronze incremental parquet drops:

from pyspark.sql import functions as F

# -----------------------------
# Config
# -----------------------------
bronze_a_glob = f"abfss://bronze@tlmbankdevadls.dfs.core.windows.net/sql/incr/bank_a_customers_*.parquet"
bronze_b_glob = f"abfss://bronze@tlmbankdevadls.dfs.core.windows.net/sql/incr/bank_b_customers_*.parquet"

silver_db_location = f"abfss://silver@tlmbankdevadls.dfs.core.windows.net/hive_metastore/silver.db"
silver_table_location = f"abfss://silver@tlmbankdevadls.dfs.core.windows.net/customers"

# -----------------------------
# Ensure DB + Table (external Delta in ADLS)
# -----------------------------
spark.sql("USE CATALOG hive_metastore")

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS hive_metastore.silver
LOCATION '{silver_db_location}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.silver.customers (
  customer_key STRING,
  customer_id STRING,
  first_name STRING,
  last_name STRING,
  dob DATE,
  country STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
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
# Read Bronze incremental drops (glob)
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
# CDM + DQ + keep latest per customer_key within the batch
# -----------------------------
stg_raw = (
    df.select(
        F.col("customer_id").cast("string").alias("customer_id"),
        F.col("first_name").cast("string").alias("first_name"),
        F.col("last_name").cast("string").alias("last_name"),
        F.to_date(F.col("dob")).alias("dob"),
        F.col("country").cast("string").alias("country"),
        F.to_timestamp(F.col("created_at")).alias("created_at"),
        F.to_timestamp(F.col("updated_at")).alias("updated_at"),
        F.col("source_system").cast("string").alias("source_system"),
    )
    .withColumn("customer_key", F.concat_ws("-", F.col("customer_id"), F.col("source_system")))
    .withColumn(
        "is_quarantined",
        (F.col("customer_id").isNull()) | (F.length(F.trim(F.col("customer_id"))) == 0) |
        (F.col("first_name").isNull()) | (F.length(F.trim(F.col("first_name"))) == 0) |
        (F.col("last_name").isNull()) | (F.length(F.trim(F.col("last_name"))) == 0) |
        (F.col("dob").isNull())
    )
)

stg = (
    stg_raw
    .withColumn(
        "_rn",
        F.row_number().over(
            # partition by business key, order by updated_at then created_at
            __import__("pyspark").sql.window.Window
            .partitionBy("customer_key")
            .orderBy(F.col("updated_at").desc_nulls_last(), F.col("created_at").desc_nulls_last())
        )
    )
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

stg.createOrReplaceTempView("customers_stg")

# -----------------------------
# SCD2 step 1: expire current rows if something changed
# -----------------------------
spark.sql("""
MERGE INTO hive_metastore.silver.customers AS target
USING customers_stg AS source
ON target.customer_key = source.customer_key AND target.is_current = true
WHEN MATCHED AND (
     target.customer_id        <> source.customer_id
  OR target.first_name         <> source.first_name
  OR target.last_name          <> source.last_name
  OR target.dob                <> source.dob
  OR target.country            <> source.country
  OR target.created_at         <> source.created_at
  OR target.updated_at         <> source.updated_at
  OR target.source_system      <> source.source_system
  OR target.is_quarantined     <> source.is_quarantined
) THEN UPDATE SET
  target.is_current = false,
  target.audit_modifieddate = current_timestamp()
""")

# -----------------------------
# SCD2 step 2: insert new current rows (new + changed)
# -----------------------------
spark.sql("""
MERGE INTO hive_metastore.silver.customers AS target
USING customers_stg AS source
ON target.customer_key = source.customer_key AND target.is_current = true
WHEN NOT MATCHED THEN INSERT (
  customer_key,
  customer_id,
  first_name,
  last_name,
  dob,
  country,
  created_at,
  updated_at,
  source_system,
  is_quarantined,
  audit_insertdate,
  audit_modifieddate,
  is_current
) VALUES (
  source.customer_key,
  source.customer_id,
  source.first_name,
  source.last_name,
  source.dob,
  source.country,
  source.created_at,
  source.updated_at,
  source.source_system,
  source.is_quarantined,
  current_timestamp(),
  current_timestamp(),
  true
)
""")

dbutils.notebook.exit("OK")