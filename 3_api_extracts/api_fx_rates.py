# Purpose: Fetch FX rates with EUR as base, enrich with currency names, write to ADLS (parquet),
#          return rows_written as string for ADF audit_end.

import io
import requests
import pandas as pd
from datetime import datetime, timezone

from pyspark.sql import functions as F

# -----------------------------
# Widgets (ADF parameters)
# -----------------------------
dbutils.widgets.text("target_container", "")
dbutils.widgets.text("target_path", "")
dbutils.widgets.text("target_file_name", "fx_rates.parquet")

# Optional controls
dbutils.widgets.text("currencies", "USD,JPY,GBP,CHF")  # comma-separated
dbutils.widgets.text("frequency", "M")                 # D or M
dbutils.widgets.text("base_currency", "EUR")           # keep EUR for your requirement

target_container = dbutils.widgets.get("target_container").strip()
target_path = dbutils.widgets.get("target_path").strip().strip("/")
target_file_name = dbutils.widgets.get("target_file_name").strip()

if not target_container or not target_path or not target_file_name:
    raise ValueError("Missing required: target_container, target_path, target_file_name")

# -----------------------------
# Currency names
# -----------------------------
CURRENCY_NAMES = {
    "USD": "US dollar",
    "JPY": "Japanese yen",
    "GBP": "Pound sterling",
    "CHF": "Swiss franc",
}

# -----------------------------
# Build ECB EXR series key and call API
# Series key dimensions (EXR): FREQ.CURRENCY.CURRENCY_DENOM.EXR_TYPE.EXR_SUFFIX
# Example: D.USD+JPY.EUR.SP00.A
# -----------------------------
cur_key = "+".join(currencies)
series_key = f"{freq}.{cur_key}.EUR.SP00.A"

url = f"https://data-api.ecb.europa.eu/service/data/EXR/{series_key}"
params = {"format": "csvdata"}

r = requests.get(url, params=params, timeout=30)
r.raise_for_status()

# ECB returns CSV with SDMX columns incl. TIME_PERIOD and OBS_VALUE
pdf = pd.read_csv(io.StringIO(r.text))

# Expected columns (may include more): CURRENCY, TIME_PERIOD, OBS_VALUE
required = {"CURRENCY", "TIME_PERIOD", "OBS_VALUE"}
missing = required - set(pdf.columns)
if missing:
    raise ValueError(f"ECB CSV missing columns: {sorted(missing)}")

# Keep latest observation per currency
pdf["TIME_PERIOD"] = pd.to_datetime(pdf["TIME_PERIOD"], errors="coerce")
pdf = pdf.dropna(subset=["TIME_PERIOD", "OBS_VALUE", "CURRENCY"])
pdf = pdf.sort_values(["CURRENCY", "TIME_PERIOD"])
latest = pdf.groupby("CURRENCY", as_index=False).tail(1)

as_of = datetime.now(timezone.utc)
latest["base_currency"] = "EUR"
latest["currency_code"] = latest["CURRENCY"].astype(str)
latest["currency_name"] = latest["currency_code"].map(CURRENCY_NAMES).fillna("")
latest["rate"] = latest["OBS_VALUE"].astype(float)
latest["obs_date"] = latest["TIME_PERIOD"].dt.date.astype(str)
latest["as_of"] = as_of

out_pdf = latest[["base_currency", "currency_code", "currency_name", "rate", "obs_date", "as_of"]]

df = spark.createDataFrame(out_pdf)

out_path = f"abfss://{target_container}@tlmbankdevadls.dfs.core.windows.net/{target_path}/{target_file_name}"

df.write.mode("overwrite").format("parquet").save(out_path)

rows_written = df.count()
dbutils.notebook.exit(str(rows_written))