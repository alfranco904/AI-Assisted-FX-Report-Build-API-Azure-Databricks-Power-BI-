# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — FX Star Schema
# MAGIC
# MAGIC Builds `Dim_Currency_Pair`, `Dim_Date_Time`, and `Fact_FX_Rate_Snapshot`
# MAGIC (grain: pair x timestamp) from `fx_lab.silver.fx_rates_clean`. This is the layer
# MAGIC Power BI reads — see `docs/02_powerbi_connection.md`.
# MAGIC
# MAGIC Surrogate keys here are `xxhash64` of the natural key (symbol / ingest_ts) rather than a
# MAGIC generated identity column — deterministic across incremental runs, which keeps the MERGE
# MAGIC below idempotent without needing a separate key-generation step. Good enough for a lab;
# MAGIC a production Kimball build would use identity columns with a proper key-lookup pattern.
# MAGIC
# MAGIC This version does the transform entirely in Spark SQL (temp views + `MERGE INTO`)
# MAGIC rather than the PySpark DataFrame API — same logic, just written as SQL statements.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS fx_lab.gold.dim_currency_pair (
        pair_key BIGINT,
        symbol STRING,
        base_currency STRING,
        quote_currency STRING
    ) USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS fx_lab.gold.dim_date_time (
        datetime_key BIGINT,
        ingest_ts TIMESTAMP,
        ingest_ts_et TIMESTAMP,
        date DATE,
        year INT,
        month INT,
        day INT,
        hour INT,
        minute INT
    ) USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS fx_lab.gold.fact_fx_rate_snapshot (
        pair_key BIGINT,
        datetime_key BIGINT,
        price DOUBLE,
        prior_price DOUBLE,
        pct_change DOUBLE
    ) USING DELTA
""")

# COMMAND ----------

# ---- Dim_Currency_Pair ----
# xxhash64(symbol) is a deterministic hash function — the same symbol always produces the
# same pair_key, which is what lets the MERGE below match on it safely across reruns.
spark.sql("""
    CREATE OR REPLACE TEMP VIEW new_pairs AS
    SELECT DISTINCT
        symbol,
        base_currency,
        quote_currency,
        xxhash64(symbol) AS pair_key
    FROM fx_lab.silver.fx_rates_clean
""")

spark.sql("""
    MERGE INTO fx_lab.gold.dim_currency_pair AS t
    USING new_pairs AS s
    ON t.pair_key = s.pair_key
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# ---- Dim_Date_Time ----
# year/month/day/hour/minute are Spark SQL's built-in date-part extraction functions —
# each just pulls that one component out of the ingest_ts timestamp.
spark.sql("""
    CREATE OR REPLACE TEMP VIEW new_datetimes AS
    SELECT DISTINCT
        ingest_ts,
        xxhash64(ingest_ts) AS datetime_key,
        from_utc_timestamp(ingest_ts, 'America/New_York') AS ingest_ts_et,
        to_date(ingest_ts) AS date,
        year(ingest_ts) AS year,
        month(ingest_ts) AS month,
        day(ingest_ts) AS day,
        hour(ingest_ts) AS hour,
        minute(ingest_ts) AS minute
    FROM fx_lab.silver.fx_rates_clean
""")

spark.sql("""
    MERGE INTO fx_lab.gold.dim_date_time AS t
    USING new_datetimes AS s
    ON t.datetime_key = s.datetime_key
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# ---- Fact_FX_Rate_Snapshot ----
# prior_price / pct_change computed per-pair over the full Silver history so the intraday
# change is correct even across incremental runs, not just within today's new rows.
#
# WITH ... AS (...) below is a CTE (Common Table Expression) — a temporary named result set
# that only exists for this one query. It's used here so prior_price only has to be computed
# once (via the LAG window function), then the outer SELECT can reuse that column directly
# instead of repeating the same OVER (PARTITION BY ...) window three times.
spark.sql("""
    CREATE OR REPLACE TEMP VIEW fact_source AS
    WITH with_prior AS (
        SELECT
            symbol,
            ingest_ts,
            price,
            LAG(price) OVER (PARTITION BY symbol ORDER BY ingest_ts) AS prior_price
        FROM fx_lab.silver.fx_rates_clean
    )
    SELECT
        xxhash64(symbol) AS pair_key,
        xxhash64(ingest_ts) AS datetime_key,
        price,
        prior_price,
        CASE
            WHEN prior_price IS NOT NULL AND prior_price != 0
                THEN (price - prior_price) / prior_price * 100
            ELSE NULL
        END AS pct_change
    FROM with_prior
""")

# WHEN MATCHED THEN UPDATE SET * handles rows whose (pair_key, datetime_key) already exist
# in Gold — needed because a later Silver run can supply a newer prior_price/pct_change for
# a snapshot that was already inserted (e.g. once more history exists to compute against).
# WHEN NOT MATCHED THEN INSERT * handles brand-new snapshots.
spark.sql("""
    MERGE INTO fx_lab.gold.fact_fx_rate_snapshot AS t
    USING fact_source AS s
    ON t.pair_key = s.pair_key AND t.datetime_key = s.datetime_key
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print("Gold MERGE complete.")

# COMMAND ----------

# ---- vw_latest_rate ----
# One row per currency pair (latest snapshot by ingest_ts), used by the Power BI "FX Spot Rates"
# table visual. QUALIFY + ROW_NUMBER() keeps just the most recent row per pair_key.
spark.sql("""
    CREATE OR REPLACE VIEW fx_lab.gold.vw_latest_rate AS
    SELECT
      p.symbol,
      f.price,
      f.pct_change,
      d.ingest_ts,
      d.ingest_ts_et
    FROM
      fx_lab.gold.fact_fx_rate_snapshot f
        JOIN fx_lab.gold.dim_currency_pair p
          ON f.pair_key = p.pair_key
        JOIN fx_lab.gold.dim_date_time d
          ON f.datetime_key = d.datetime_key
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY f.pair_key ORDER BY d.ingest_ts DESC) = 1
""")

print("vw_latest_rate refreshed.")

# COMMAND ----------

display(spark.sql("""
    SELECT p.symbol, d.ingest_ts, f.price, f.pct_change
    FROM fx_lab.gold.fact_fx_rate_snapshot f
    JOIN fx_lab.gold.dim_currency_pair p ON f.pair_key = p.pair_key
    JOIN fx_lab.gold.dim_date_time d ON f.datetime_key = d.datetime_key
    ORDER BY d.ingest_ts DESC
    LIMIT 20
"""))
