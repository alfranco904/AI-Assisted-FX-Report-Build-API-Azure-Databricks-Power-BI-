# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — FX Rate Ingestion
# MAGIC
# MAGIC Polls the Twelve Data FX price endpoint on a bounded loop and appends each raw response,
# MAGIC as-is, to `fx_lab.bronze.fx_raw_snapshots`. Nothing is reshaped here — that's Silver's job.
# MAGIC
# MAGIC Run this interactively during development (small `duration_minutes`), or as a scheduled
# MAGIC Databricks Job task (see `config/job_definitions.md`). Either way it runs on **Job Compute**
# MAGIC when scheduled, and terminates when the loop below finishes — see the architecture doc's
# MAGIC cost-control note.

# COMMAND ----------

# dbutils.widgets.text(name, default_value, label) creates a notebook input widget —
# a text box shown at the top of the Databricks notebook UI. This lets you change
# these values between runs (e.g. shorten duration_minutes for a quick test) without
# editing the code itself. Each call registers one widget; it's a no-op if the widget
# already exists from a previous run.
dbutils.widgets.text("watchlist", "EUR/USD,GBP/USD,USD/JPY,AUD/USD,USD/CAD", "Currency pairs (comma-separated)")
dbutils.widgets.text("poll_interval_seconds", "60", "Seconds between polls")
dbutils.widgets.text("duration_minutes", "10", "Total run length (minutes)")

# dbutils.widgets.get(name) reads back the widget's current value — whatever is typed
# into the box in the notebook UI, or the default if untouched. Widget values are
# always returned as strings, so the numeric ones need casting below.
WATCHLIST = dbutils.widgets.get("watchlist")
POLL_INTERVAL_SECONDS = int(dbutils.widgets.get("poll_interval_seconds"))
DURATION_MINUTES = int(dbutils.widgets.get("duration_minutes"))

# COMMAND ----------

# Standard library / third-party imports — plain Python, nothing Databricks-specific here.
import requests   # HTTP client used to call the Twelve Data REST API
import time       # time.time() / time.sleep() drive the polling loop's timing below
import json       # serializes each API response into a string for the raw_json column
from datetime import datetime, timezone   # generates a UTC timestamp per poll
from pyspark.sql import Row               # Spark's row constructor, used to build DataFrame rows
from pyspark.sql.types import StructType, StructField, StringType, TimestampType  # explicit schema types

# dbutils.secrets.get(scope, key) fetches a secret value from the named Databricks secret
# scope — here, "fx-lab", which is backed by the kv-fx-rates-lab Key Vault. This keeps the
# API key out of the notebook source entirely (never hardcoded, never shown in plain text
# in results/logs the way a literal string would be).
API_KEY = dbutils.secrets.get(scope="fx-lab", key="twelvedata-api-key")
BASE_URL = "https://api.twelvedata.com/price"

# Explicit schema — error_message is None on every row of a successful poll, and Spark
# can't infer a type from an all-null column, so createDataFrame(rows) alone fails with
# CANNOT_DETERMINE_TYPE the first time a poll succeeds cleanly.
BRONZE_SCHEMA = StructType([
    StructField("ingest_ts", TimestampType(), True),
    StructField("symbol", StringType(), True),
    StructField("raw_json", StringType(), True),
    StructField("error_message", StringType(), True),
])


def fetch_batch(symbols: str) -> dict:
    """One call to Twelve Data covering the whole watchlist — stays well inside the free-tier
    rate limit (8 req/min) regardless of how many symbols are in the watchlist."""
    # requests.get(url, params=..., timeout=10) issues the HTTP GET; `params` becomes the
    # query string (e.g. ?symbol=EUR/USD,GBP/USD&apikey=...). timeout=10 means the call
    # raises if the server doesn't respond within 10 seconds, instead of hanging forever.
    resp = requests.get(BASE_URL, params={"symbol": symbols, "apikey": API_KEY}, timeout=10)
    # raise_for_status() throws an exception if the HTTP status is 4xx/5xx (e.g. a 429
    # rate-limit response) — this is what gets caught by the try/except in the polling
    # loop below and turned into a quarantined row instead of crashing the notebook.
    resp.raise_for_status()
    # .json() parses the response body from a JSON string into a Python dict.
    return resp.json()


def normalize(raw: dict, symbols: str) -> list[dict]:
    """Twelve Data returns {"price": "..."} for a single symbol but
    {"EUR/USD": {"price": "..."}, ...} for multiple. Normalize to one shape either way."""
    # Splits the comma-separated watchlist string back into individual symbols, e.g.
    # "EUR/USD,GBP/USD" -> ["EUR/USD", "GBP/USD"].
    symbol_list = symbols.split(",")
    # Single-symbol case: Twelve Data's response IS the price dict directly (no per-symbol
    # nesting), so pair it as-is with the one symbol we requested.
    if len(symbol_list) == 1:
        return [{"symbol": symbol_list[0], "raw": raw}]
    # Multi-symbol case: the response is keyed by symbol, e.g. raw["EUR/USD"] = {"price": ...}.
    # Look up each requested symbol's entry (raw.get(sym, {}) so a symbol missing from the
    # response — e.g. Twelve Data silently dropped it — becomes an empty dict rather than
    # a KeyError, which the Silver notebook will later quarantine as a missing price field).
    return [{"symbol": sym, "raw": raw.get(sym, {})} for sym in symbol_list]

# COMMAND ----------

end_time = time.time() + (DURATION_MINUTES * 60)
poll_count = 0

while time.time() < end_time:
    ingest_ts = datetime.now(timezone.utc)
    try:
        raw_response = fetch_batch(WATCHLIST)
        records = normalize(raw_response, WATCHLIST)
        error_message = None
    except Exception as exc:  # network hiccup or rate-limit response — land it, don't crash the loop
        records = [{"symbol": sym, "raw": {}} for sym in WATCHLIST.split(",")]
        # str(exc) on a failed request includes the full request URL, which embeds API_KEY
        # as a query param — redact it before this ever reaches print()/Bronze/Silver.
        error_message = str(exc).replace(API_KEY, "[REDACTED]")

    rows = [
        Row(
            ingest_ts=ingest_ts,
            symbol=r["symbol"],
            raw_json=json.dumps(r["raw"]),
            error_message=error_message,
        )
        for r in records
    ]

    df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
    df.write.format("delta").mode("append").saveAsTable("fx_lab.bronze.fx_raw_snapshots")

    poll_count += 1
    print(f"[{ingest_ts.isoformat()}] poll #{poll_count}: wrote {len(rows)} rows"
          + (f" (error: {error_message})" if error_message else ""))

    time.sleep(POLL_INTERVAL_SECONDS)

print(f"Done — {poll_count} polls over {DURATION_MINUTES} minutes.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check
# MAGIC Run this cell after the loop finishes (or in a separate notebook cell while it's running)
# MAGIC to confirm rows are landing.

# COMMAND ----------

display(
    spark.sql("""
        SELECT symbol, ingest_ts, raw_json, error_message
        FROM fx_lab.bronze.fx_raw_snapshots
        ORDER BY ingest_ts DESC
        LIMIT 20
    """)
)
