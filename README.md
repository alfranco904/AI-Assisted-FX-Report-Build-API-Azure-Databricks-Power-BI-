# FX Rates Near-Real-Time Pipeline — Project Overview

An API ingestion BI lab built on Azure Databricks: a live FX currency-pair feed is ingested,
cleaned, and modeled through a bronze / silver / gold lakehouse, then served to a near-real-time
Power BI dashboard. Built end-to-end — cloud provisioning, ingestion, transformation, orchestration,
and reporting — as a hands-on demonstration of a modern lakehouse pattern applied to market data.

## What it does

Every N seconds, a scheduled job polls a live FX rates API for a watchlist of currency pairs
(majors: EUR/USD, GBP/USD, USD/JPY, USD/CHF, AUD/USD, USD/CAD, NZD/USD, plus EUR/JPY as the most
liquid cross pair). Each response lands as raw JSON, gets cleaned and typed, then reshaped into a
proper star schema that Power BI queries live — so the dashboard's line chart and rate table move
as new data arrives, without any manual refresh step.

## Architecture

```
Twelve Data REST API
        │  (batched HTTP poll, one call per interval)
        ▼
┌─────────────────────────────────────────────────────────┐
│  Azure Databricks (Unity Catalog, ADLS Gen2 / Delta)     │
│                                                           │
│   Bronze  →  Silver  →  Gold                             │
│   raw JSON   cleaned/    star schema                     │
│   as-is      typed rows  (Fact + Dims)                   │
└─────────────────────────────────────────────────────────┘
        │
        ▼
  Databricks SQL Warehouse (Serverless)
        │  DirectQuery (ODBC/JDBC)
        ▼
   Power BI Dashboard
```

Orchestrated end-to-end as a single Databricks Job (`fx_rates_pipeline`), triggered on a schedule,
with each stage running on ephemeral Job Compute that auto-terminates the moment its task
finishes — no compute is left running (and billing) between scheduled runs.

![Job trigger schedule](/images/job_trigger_schedule.png)

![Pipeline Run](/images/pipeline_run.png)


## Pipeline stages

**Bronze — `01_bronze_ingest.py`**
Polls the API on a bounded loop (configurable poll interval and total duration) and appends every
raw response, untouched, to `fx_lab.bronze.fx_raw_snapshots`. Failed polls (network errors, API
rate-limit responses) are captured as error rows rather than crashing the run, with any credential
data stripped from the stored error message before it's written.

**Silver — `02_silver_transform.py`**
Reads new Bronze rows (via a watermark, so each run is incremental, not a full reprocess), parses
the JSON payload, splits each symbol into base/quote currency, and casts price to a proper numeric
type. Rows that fail to parse are quarantined into a separate table instead of being silently
dropped, so nothing disappears without a trace. Loads via `MERGE INTO` matched on
`(symbol, ingest_ts)`, making the whole step idempotent — a retried run can't double-insert data.

**Gold — `03_gold_star_schema.py`**
Builds a Kimball-style star schema: `Dim_Currency_Pair`, `Dim_Date_Time`, and
`Fact_FX_Rate_Snapshot` (grain: one row per pair per timestamp), including a windowed
`prior_price`/`pct_change` calculation over each pair's full history. Surrogate keys are
deterministic hashes of the natural key, so the same MERGE-based idempotency holds here too.

All three transform stages are written in pure Spark SQL (temp views + `MERGE INTO` / CTEs +
window functions) rather than the PySpark DataFrame API — a deliberate choice to keep the logic
in a form that reads like standard SQL.

## Dashboard

Power BI connects to the Gold layer via the SQL Warehouse in DirectQuery mode, so visuals query
live rather than against a static import:

- **FX Rate Movement** — line chart of price over time per pair, the core "watch the rate move" view
- **FX Spot Rates** — table of each pair's latest price and percent change, sourced from a
  dedicated `vw_latest_rate` view (one row per pair via `ROW_NUMBER() ... QUALIFY`)
- A day slicer lets a specific session's data be viewed in isolation, independent of any other
  day's accumulated history in the same tables

![FX Rate Movement line chart and FX Spot Rates Table](/images/dashboard_line_chart1.png)
![Selected Currency Pair 1](/images/dashboard_line_chart2.png)
![Selected Currency Pair 2](/images/dashboard_line_chart3.png)


## Engineering notes worth calling out

- **Rate-limit-aware ingestion.** The upstream API's free tier caps out at 8 credits/minute and
  800/day, billed *per symbol requested*, not per HTTP call. Getting a batched multi-symbol poll
  to actually fit inside the per-minute cap (rather than just reducing HTTP call count, which
  doesn't reduce billed credits) required trimming the watchlist to fit under the limit and sizing
  the poll interval against the daily cap — the kind of constraint that only surfaces once you're
  running against a real, metered external API rather than a mocked one.
- **Idempotent by construction.** Every load from Silver onward uses `MERGE INTO` matched on a
  natural or derived key, so re-running any stage against already-processed data is a no-op rather
  than a duplicate.
- **Nothing fails silently.** Bad or unparseable rows are quarantined with a reason, not dropped —
  the pipeline can be audited for data quality after the fact.
- **Credentials never touch notebook source or logs.** The API key is pulled from a Key Vault-backed
  Databricks secret scope at runtime, and any exception text that could embed it in a request URL
  is redacted before being written to any table.
- **Cost-bounded by design.** Every task runs on ephemeral Job Compute that terminates as soon as
  its notebook finishes — there's no cluster left idling (and billing) between scheduled runs.
