# Retail Data Engineering Pipeline

A portfolio batch ELT project built around the UCI Online Retail dataset. It reads the source workbook or CSV, lands a fingerprinted raw copy, validates rows, writes Bronze and batch-level Silver Parquet, and maintains incremental Gold aggregates in PostgreSQL when enabled.

## Architecture

```text
UCI Online Retail XLSX/CSV
          │
          ├── raw/ (fingerprinted source copy; ignored by Git)
          ▼
PySpark batch job ──► Bronze Parquet (source fields + ingestion metadata)
          ├── invalid rows ──► quarantine Parquet + reason codes
          ▼
Silver batch Parquet (clean rows for this fingerprint)
          │
          └── PostgreSQL current line state + transactional batch ledger
                    └── refresh affected Gold groups in one transaction
                               ├── Gold tables in PostgreSQL
                               └── compact Gold Parquet snapshots

With PostgreSQL disabled, the local fallback merges Silver batches and rebuilds Gold.
```

Parquet is a column-oriented file format, not a warehouse. PySpark performs the transformations; PostgreSQL is the queryable analytics warehouse. The layered directories are a small local medallion-style lake. This demo does not claim cluster-scale throughput.

## Problem statement

Retail transaction exports arrive as semi-structured spreadsheet or CSV files. Analysts need consistent typed records, traceable rejects, repeatable batch processing, and query-ready daily, product, and country summaries. This project demonstrates that local ELT workflow while retaining source lineage and preventing duplicate warehouse application on retries.

## Decisions and data rules

- Source fields: `InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID`, `Country`.
- Missing required identifiers/date/country, invalid timestamps or nonnumeric measures, and zero quantity/negative unit price are quarantined. Negative quantities are retained as returns; invoice numbers beginning with `C` are marked as cancellations/returns rather than discarded.
- Silver calculates `line_amount = quantity * unit_price` and a stable SHA-256 `line_id`. UCI has no line ID, so the key hashes the complete normalized event fields; exact repeats deduplicate while changes to quantity/price remain distinct events.
- Each input file is a batch identified by its SHA-256 fingerprint. The JSON manifest skips completed files; PostgreSQL also keeps a transactional `pipeline_batches` ledger, so forced retries of an applied batch do not reapply it. New batches write only their incoming Silver rows, upsert current line state, and refresh affected Gold groups; the historical fact table is scanned only for affected groups. The first PostgreSQL incremental run can seed its fact table from the existing Silver snapshot.
- In warehouse mode, late-arriving events and returns recalculate only their affected date, product, and country groups from current PostgreSQL line state. Gold Parquet snapshots are refreshed from the compact aggregate tables. The first run can seed current line state from the previous Silver snapshot; later batches do not scan all historical Silver.
- In warehouse-enabled mode, PostgreSQL stores current line state and current Gold aggregates. Each batch, its line upserts, and its affected Gold-key refreshes commit in one transaction. Gold Parquet is synchronized from the compact aggregate tables. Credentials come from environment variables.
- The UCI workbook has no line ID. Exact event rows use a hash of normalized business fields, preserving distinct timestamps, quantities, prices, and returns while making repeated events idempotent. A source that supplies `LineID`, `line_id`, or `LineItemID` uses that stable key to update a corrected row; conflicting versions of one supplied ID within a batch are quarantined. Without a source line ID, represent a correction as a compensating event and replacement; the pipeline cannot infer which of two otherwise similar retail lines was edited.
- The UCI workbook is not included. Download it from the [UCI Online Retail dataset page](https://archive.ics.uci.edu/dataset/352/online+retail) and put it at `data/input/online_retail.xlsx`.

## Project layout

```text
src/retail_pipeline/       ingestion, quality, transforms, warehouse, orchestration stages
airflow/dags/              Airflow DAG for scheduled, retryable batch execution
airflow/tests/             DAG import and dependency checks (Airflow environment)
sql/                       warehouse DDL and example analytics
scripts/                   repeatable Spark benchmark and disposable-database verifiers
tests/                     focused unit tests
data/                      local input and Parquet layers (ignored)
logs/                      run logs (ignored)
```

## Technology stack

| Technology | Purpose in this project |
| --- | --- |
| Python | CLI, configuration, validation rules, orchestration glue, and audit records |
| PySpark | DataFrame based ingestion, cleansing, deduplication, and aggregation on a local Spark master |
| Parquet | Compressed, column-oriented persistence for Bronze, Silver, quarantine, and Gold snapshots |
| PostgreSQL | Current line state, transactional batch ledger, and queryable Gold aggregates |
| Apache Airflow | Optional local scheduling, task dependencies, retries, and run history; it is not required for manual runs |
| pytest | Unit and Spark fixture tests; a separate disposable database script covers the end-to-end warehouse lifecycle |

## Bronze, Silver, and Gold

| Layer | Contents | Trust/use |
| --- | --- | --- |
| Bronze | Source-shaped rows plus filename, fingerprint, run ID, ingest time, and additive source fields | Immutable batch landing area for traceability |
| Silver | Typed, validated, deduplicated rows with stable line IDs, derived amounts/dates, and preserved unmapped extras | Clean batch facts used for aggregates and warehouse upserts |
| Gold | Daily, product, and country aggregates in PostgreSQL and compact Parquet snapshots | Analytics ready summaries; negative quantity returns reduce net revenue |

Invalid records are persisted separately in the quarantine Parquet layer with one or more reason codes.

## ELT workflow and warehouse design

1. Fingerprint and retain the source batch; stream XLSX to CSV for Spark input.
2. Write source fields and lineage to Bronze.
3. Standardize types and validate business rules; reconcile accepted plus rejected rows against source count.
4. Preserve rejects in quarantine and write clean, deduplicated batch rows to Silver.
5. In PostgreSQL mode, stage the incoming lines, upsert by `line_id`, refresh only impacted Gold keys, and commit line state, aggregates, and batch ledger together.
6. Read compact Gold tables back to Parquet, verify totals, and write the atomic JSON audit/manifest record.

`retail_line_items` is the current fact table keyed by `line_id`; `pipeline_batches` is the idempotency ledger keyed by source fingerprint. `daily_sales` uses `(sale_date, country)`, `product_sales` uses `(stock_code, description)`, and `country_sales` uses `country` as their primary keys. The DDL and indexes are in `sql/schema.sql`; example analytics and query plans are in `sql/analytics.sql` and `sql/performance.sql`.

## Setup

Requires Python 3.10+, Java 17 (recommended for this PySpark 3.5 project), and PostgreSQL only if using the warehouse target. On Windows, local Spark file writes also require Hadoop Windows helper binaries matching the Hadoop version bundled with PySpark (this environment bundles Hadoop 3.3.4): `winutils.exe` and `hadoop.dll` under `%HADOOP_HOME%\bin`. Apache does not publish these Windows helper binaries; use a trusted community build and do not mix versions.

```powershell
cd work/retail-data-pipeline
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
```

Set `INPUT_PATH` in `.env` or pass `--input`. Copy `.env.example` to `.env` and fill credentials only when PostgreSQL is enabled. Never commit `.env` or raw data.

## Run

```powershell
python -m retail_pipeline --input data/input/online_retail.xlsx
```

Without PostgreSQL configuration, the job completes through Gold Parquet. To enable PostgreSQL on Windows:

1. Open the Windows Services app, find the installed PostgreSQL 18 service, and start it. The pipeline needs the server running locally.
2. Open SQL Shell (`psql`). Connect to `localhost` on port `5432`, database `postgres`, user `postgres`, and enter the password you set when installing PostgreSQL. At the SQL prompt, create the project database:

   ```sql
   CREATE DATABASE retail;
   ```

3. Exit SQL Shell. From the project folder, connect to the new `retail` database and run the schema file:

   ```powershell
   & 'C:\Program Files\PostgreSQL\18\bin\psql.exe' -U postgres -h localhost -p 5432 -d retail -f sql/schema.sql
   ```

4. Copy `.env.example` to `.env`. Set `WAREHOUSE_ENABLED=true`, `PGHOST=localhost`, `PGPORT=5432`, `PGDATABASE=retail`, `PGUSER=postgres`, and `PGPASSWORD` to the password you chose for PostgreSQL. Keep `.env` local; it is ignored by Git.
5. From the project folder with `.venv` active, run `python -m retail_pipeline --input data/input/online_retail.xlsx --force`. The earlier Parquet-only run marked this input complete, so `--force` is needed to run it again and load Gold into PostgreSQL.


`--dry-run` executes transformations but skips the warehouse write and completion manifest.

For XLSX input, the job streams rows into an ignored CSV beside the raw workbook before Spark reads it; the first conversion can take a little time. Console output shows progress, while `logs/pipeline.log` stores JSON-lines events with a run ID, batch ID, stage, and metrics. Each run writes an atomic JSON audit record to `data/audit/`, including failed runs and their error stage. Quarantined rows keep one or more reason codes. The pipeline checks source = accepted + quarantined, verifies persisted Parquet row counts, and reconciles Silver/Gold net revenue where the local full-rebuild path is used; warehouse mode also reconciles impacted fact groups against Gold inside its transaction.

## Test

The core automated suite uses in-memory fixtures and Spark. It does not download UCI data or write to PostgreSQL. Run it in the project environment:

```powershell
python -m pytest
```

The WSL core suite passed 11 tests and the Airflow DAG suite passed 1 test on 2026-10-31. The Phase 10 warehouse scenario runner in [`docs/PHASE_10_VALIDATION.md`](docs/PHASE_10_VALIDATION.md) also passed against a uniquely named disposable database; it verified all scenarios without using the configured `retail` database.

The 2026-10-31 local benchmark on the full 541,909-row workbook measured a 105.3 second median at 4 shuffle partitions versus 109.7 seconds at 8, with 20 versus 24 Parquet files. This modest local result is not a distributed performance claim. See [`docs/PHASE_9_ENGINEERING.md`](docs/PHASE_9_ENGINEERING.md) for the full comparison and limits.

## Orchestrate with Airflow

The Airflow DAG runs ingestion → transformation/quarantine → warehouse/Gold publication. It retries failed tasks, skips source fingerprints already marked complete, and uses the PostgreSQL batch ledger to protect retries after a commit. For installation and WSL2 setup, see [`docs/PHASE_8_ORCHESTRATION.md`](docs/PHASE_8_ORCHESTRATION.md). Airflow is intentionally kept out of the Windows `.venv` used for manual pipeline runs.


## Incremental acceptance check

For an existing project that has not migrated its data into `retail_line_items`, the first run must use `--force` to seed that table from the previous Silver Parquet snapshot. After that batch commits, repeating it with `--force` is skipped by the PostgreSQL batch ledger before Spark starts. Row counts in `retail_line_items`, `pipeline_batches`, and Gold should remain unchanged. A normal repeat without `--force` is skipped by the file manifest before Spark starts.

## Analytics

See `sql/analytics.sql` for monthly revenue, top products, country performance and return-rate examples. Revenue is net of negative quantity rows. For gross sales, filter out negative quantities explicitly.

## Interview walkthrough

See [`docs/INTERVIEW_GUIDE.md`](docs/INTERVIEW_GUIDE.md) for a short project pitch and likely interview questions. [`docs/PHASE_10_VALIDATION.md`](docs/PHASE_10_VALIDATION.md) contains the final acceptance matrix and run instructions. See [`docs/PHASE_5_AUDIT.md`](docs/PHASE_5_AUDIT.md) for the verified baseline and gap analysis, [`docs/PHASE_6_INCREMENTAL.md`](docs/PHASE_6_INCREMENTAL.md) for the incremental design, [`docs/PHASE_8_ORCHESTRATION.md`](docs/PHASE_8_ORCHESTRATION.md) for Airflow setup, and [`docs/PHASE_9_ENGINEERING.md`](docs/PHASE_9_ENGINEERING.md) for schema evolution, backfills, lineage, measured tuning, and query plans.

## Limitations

This is an interview-ready local batch reference, not a production service. Airflow is provided as a local learning DAG; it does not include cloud storage, a secrets manager, schema registry, or distributed deployment. Spark runs locally by default. PostgreSQL line batches are streamed through the driver and suit this dataset; very large data would use bulk staging and database-side merges. The PostgreSQL incremental path refreshes affected aggregate keys, while the no-warehouse fallback rebuilds local Silver/Gold. Data quality checks are explicit and tested, but there is no external data-quality framework.
