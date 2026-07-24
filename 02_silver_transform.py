# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Clean FX Rates
# MAGIC
# MAGIC Reads new rows from `fx_lab.bronze.fx_raw_snapshots`, parses and types them, and
# MAGIC `MERGE`s into `fx_lab.silver.fx_rates_clean`. Rows that fail to parse (API errors,
# MAGIC missing price field) are quarantined into `fx_lab.silver.fx_rates_quarantine`
# MAGIC instead of being silently dropped.
# MAGIC
# MAGIC Runs as a scheduled Job Compute task, downstream of Bronze — see `config/job_definitions.md`.
# MAGIC
# MAGIC This version does the transform entirely in Spark SQL (temp views + `MERGE INTO`)
# MAGIC rather than the PySpark DataFrame API — same logic, just written as SQL statements.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS fx_lab.silver.fx_rates_clean (
        symbol STRING,
        base_currency STRING,
        quote_currency STRING,
        price DOUBLE,
        ingest_ts TIMESTAMP
    ) USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS fx_lab.silver.fx_rates_quarantine (
        symbol STRING,
        raw_json STRING,
        error_message STRING,
        ingest_ts TIMESTAMP,
        quarantine_reason STRING
    ) USING DELTA
""")

# COMMAND ----------

# Watermark: the latest ingest_ts already landed in EITHER downstream table. Only Bronze
# rows newer than this need to be looked at — keeps every run incremental instead of
# reprocessing the whole Bronze history each time.
watermark = spark.sql("""
    SELECT GREATEST(
        COALESCE((SELECT MAX(ingest_ts) FROM fx_lab.silver.fx_rates_clean), TIMESTAMP('1970-01-01')),
        COALESCE((SELECT MAX(ingest_ts) FROM fx_lab.silver.fx_rates_quarantine), TIMESTAMP('1970-01-01'))
    ) AS watermark
""").collect()[0]["watermark"]

print(f"Watermark: {watermark}")

# COMMAND ----------

# Everything below chains together with named temp views instead of DataFrame variables —
# each CREATE OR REPLACE TEMP VIEW gives a SQL query a name so the next query can SELECT
# FROM it, same as building a view in the SQL editor.

spark.sql(f"""
    SELECT *
    FROM fx_lab.bronze.fx_raw_snapshots
    WHERE ingest_ts > TIMESTAMP('{watermark}')
""").createOrReplaceTempView("new_bronze")

new_count = spark.sql("SELECT COUNT(*) AS n FROM new_bronze").collect()[0]["n"]
print(f"{new_count} new Bronze rows to process")

# COMMAND ----------

# Parse each row's price out of raw_json. get_json_object(col, '$.price') pulls the value
# at that JSON path out as a string; CAST(... AS DOUBLE) converts it to a number (or NULL
# if it wasn't a valid number / wasn't present at all).
spark.sql("""
    CREATE OR REPLACE TEMP VIEW parsed_bronze AS
    SELECT
        symbol,
        raw_json,
        error_message,
        ingest_ts,
        CAST(get_json_object(raw_json, '$.price') AS DOUBLE) AS parsed_price
    FROM new_bronze
""")

# Clean rows: no Bronze-recorded error, and the price parsed to a real number.
# split(symbol, '/')[0]/[1] breaks "EUR/USD" into base_currency = "EUR", quote_currency = "USD".
spark.sql("""
    CREATE OR REPLACE TEMP VIEW good_rows AS
    SELECT
        symbol,
        split(symbol, '/')[0] AS base_currency,
        split(symbol, '/')[1] AS quote_currency,
        parsed_price AS price,
        ingest_ts
    FROM parsed_bronze
    WHERE error_message IS NULL AND parsed_price IS NOT NULL
""")

# Bad rows: either Bronze already recorded an API error, or nothing parsed to a number.
# COALESCE keeps the real error message when there is one, otherwise labels it as a
# parsing failure — same fallback idea as the watermark's COALESCE above.
spark.sql("""
    CREATE OR REPLACE TEMP VIEW bad_rows AS
    SELECT
        symbol,
        raw_json,
        error_message,
        ingest_ts,
        COALESCE(error_message, 'missing or non-numeric price field') AS quarantine_reason
    FROM parsed_bronze
    WHERE error_message IS NOT NULL OR parsed_price IS NULL
""")

good_count = spark.sql("SELECT COUNT(*) AS n FROM good_rows").collect()[0]["n"]
bad_count = spark.sql("SELECT COUNT(*) AS n FROM bad_rows").collect()[0]["n"]
print(f"Clean: {good_count}   Quarantined: {bad_count}")

# COMMAND ----------

# MERGE INTO ... WHEN NOT MATCHED THEN INSERT — Delta Lake's SQL upsert statement. Matching
# on (symbol, ingest_ts) means re-running this notebook against the same Bronze rows inserts
# nothing new the second time, so a retried Job task can't double-insert data.
spark.sql("""
    MERGE INTO fx_lab.silver.fx_rates_clean AS t
    USING good_rows AS s
    ON t.symbol = s.symbol AND t.ingest_ts = s.ingest_ts
    WHEN NOT MATCHED THEN INSERT *
""")

spark.sql("""
    MERGE INTO fx_lab.silver.fx_rates_quarantine AS t
    USING bad_rows AS s
    ON t.symbol = s.symbol AND t.ingest_ts = s.ingest_ts
    WHEN NOT MATCHED THEN INSERT *
""")

print("Silver MERGE complete.")

# COMMAND ----------

display(spark.sql("SELECT * FROM fx_lab.silver.fx_rates_clean ORDER BY ingest_ts DESC LIMIT 20"))
