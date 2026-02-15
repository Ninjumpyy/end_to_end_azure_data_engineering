# Purpose: Read incremental Bronze parquet files and return MAX(watermark_column)
# Returns: dbutils.notebook.exit("<new_watermark>") or "" if no rows

from pyspark.sql import functions as F

# ---- Widgets from ADF ----
dbutils.widgets.text("container", "")
dbutils.widgets.text("file_path", "")
dbutils.widgets.text("file_name", "")
dbutils.widgets.text("watermark_column", "")

container = dbutils.widgets.get("container").strip()
target_path = dbutils.widgets.get("file_path").strip()
file_name = dbutils.widgets.get("file_name").strip()
watermark_column = dbutils.widgets.get("watermark_column").strip()

if not container or not target_path or not watermark_column:
    raise ValueError("container, target_path and watermark_column are required")

# ---- Build incremental path ----
base_path = f"abfss://{container}@tlmbankdevadls.dfs.core.windows.net/{target_path}/{file_name}"

try:
    df = spark.read.format("parquet").load(base_path)

    if watermark_column not in df.columns:
        raise ValueError(f"{watermark_column} not found in data")

    result = (
        df.agg(F.max(F.col(watermark_column)).alias("max_wm"))
          .collect()
    )

    new_wm = result[0]["max_wm"]

    # If no rows or max is null
    if new_wm is None:
        dbutils.notebook.exit("")
    else:
        dbutils.notebook.exit(str(new_wm))

except Exception:
    # If folder does not exist or no data
    dbutils.notebook.exit("")