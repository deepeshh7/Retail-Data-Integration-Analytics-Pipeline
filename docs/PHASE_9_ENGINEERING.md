# Phase 9 — Local pipeline engineering improvements

## Scope and status

This phase improves the existing local pipeline; it does not deploy cloud services. It builds on the batch fingerprinting, Bronze/Silver/Gold layers, PostgreSQL ledger, quality quarantine, audit logs, and Airflow DAG already in the project.

**Status: Complete and verified** for the local Phase 9 scope. See the verification record below. The isolated backfill database remains available for review; the production-style Windows `retail` database was not used by the integration check.

| Area | Current behavior after Phase 9 | Boundary |
|---|---|---|
| Additive schema changes | Required source fields remain strict; new columns are captured in `_source_extras` JSON and carried through Bronze and Silver. Audit records list the extra column names and schema version. | Extras are preserved but ignored by calculations until explicitly mapped and validated. |
| Backfill | `--backfill-batch <sha256>` reprocesses a retained raw batch. Original batch ordering is retained for existing rows so a replay cannot outrank a later correction. PostgreSQL mode rebuilds current facts and Gold in one transaction from all retained Silver batches. | A source file must still exist under `data/raw/<fingerprint>/`. This is a full rebuild, so it is heavier than a normal incremental run. |
| Lineage | Bronze/Silver rows retain source file, source fingerprint, ingest timestamp, and run ID; batch records retain counts, schema metadata, rejection reasons, and outcomes. | No external lineage catalog is used. |
| Spark tuning | Shuffle partition count and maximum rows per Parquet file are configurable. The local default is now 4 shuffle partitions; the Parquet row cap remains disabled. | The measured gain is modest and applies to this local WSL run and dataset only; it is not a distributed-scale claim. |
| PostgreSQL query tuning | Existing indexes cover common date/country/product lookups. `sql/performance.sql` runs `ANALYZE` and representative `EXPLAIN (ANALYZE, BUFFERS)` queries. | Index choices must be based on actual plans and workload; small tables can correctly use sequential scans. |
| Credentials | PostgreSQL credentials come from environment variables or the ignored local `.env` file. | Do not commit `.env`, passwords, or raw data. |
| Monitoring | JSON-line logs and atomic audit records include run/batch IDs, stage, counts, schema metadata, duration, status, and failures. | This is local logging, not alerting or centralized monitoring. |

## Additive schema evolution policy

`InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID`, and `Country` are required. Recognized line-ID columns remain supported. Other columns are treated as additive fields: ingestion preserves their values as JSON in `_source_extras`, records their names in `_source_extra_columns`, and records schema contract version 1. This makes a new `Discount` column visible and recoverable without silently treating it as part of revenue.

Before an additive field affects business measures, update the contract deliberately: define its accepted types and null rules, map it into Silver, add quality checks, decide whether it changes stable line identity, update warehouse/Gold definitions if needed, and add tests. A missing required column continues to fail the batch clearly.

## Reprocess a historical batch

First locate the fingerprint in an audit record under `data/audit/`. Then run from the project directory with the project environment active:

```powershell
python -m retail_pipeline --backfill-batch <64-character-sha256>
```

For example, replace the placeholder with the `batch_id` value from the audit JSON. The source file must remain in `data/raw/<batch_id>/`. The job bypasses completed-batch skip checks, repeats Bronze → Silver validation for that source, and rebuilds Gold. A replay preserves the batch's original `_ingested_at` ordering for rows already present in its Silver batch while recording the new run ID; otherwise replaying an old batch could make an old version appear newer than a later correction. With PostgreSQL enabled, it gathers retained Silver batches, resolves each stable `line_id` to its latest ingested version, then replaces current facts and Gold atomically. If the transaction fails, PostgreSQL rolls the rebuild back. The batch ledger records the refreshed batch.

This is intentionally a heavy, explicit repair operation. Normal runs still use the incremental path. Backfill does not recreate source files that have been deleted, and a source without a stable line ID cannot express a correction as an update unless the corrected event replaces its prior event consistently.

To verify the incremental correction plus repeatable historical replay against an isolated PostgreSQL database, run the disposable integration check from Ubuntu/WSL:

```bash
source ~/airflow-retail/.venv/bin/activate
python scripts/verify_postgres_backfill.py
```

It requires PostgreSQL credentials from the local `.env` and permission for that user to create databases. The script creates a unique `retail_phase9_verify_<timestamp>` database, applies `sql/schema.sql`, loads two tiny fixture batches with a stable line ID correction, replays the first batch twice, and checks fact uniqueness, current corrected quantity, Gold totals, and batch counts at each point. It never connects the pipeline to the configured `retail` database. It leaves the scratch database and small fixture artifacts for manual review; cleanup is deliberately manual.

## Measure Spark settings instead of guessing

Two local settings can be varied without changing pipeline code:

- `SPARK_SHUFFLE_PARTITIONS` (default `4`; the benchmark baseline was `8`)
- `SPARK_MAX_RECORDS_PER_FILE` (default `0`, preserving Spark's prior behavior; positive values set a row limit)

Use the same source file, machine, Java version, and warehouse mode, with a fresh `DATA_DIR` for each trial. From the project directory in Ubuntu/WSL, activate the project environment and run the included controlled benchmark:

```bash
source ~/airflow-retail/.venv/bin/activate
python scripts/benchmark_spark.py
```

The script runs two trials each for the current baseline (8 shuffle partitions, no row cap), 4 and 12 partitions, and a 100,000-row Parquet cap at 8 partitions. It interleaves the trials to reduce ordering bias, uses a unique data directory for every run, disables PostgreSQL, measures wall-clock time, and records pipeline duration, row counts, and aggregate Parquet file count/bytes. It writes detailed CSV results and trial logs under ignored `data/bench/` and `logs/bench/` directories. After collecting each successful trial's metrics, it removes only that newly created trial output directory to avoid retaining multiple copies of all layers and the source. Each run executes the full Spark pipeline with `--dry-run`; dry-run skips PostgreSQL and the completion manifest, but still writes and reconciles local Bronze, quarantine, Silver, and Gold outputs.

### Recorded local benchmark (2026-10-31)

The full UCI workbook was run in Ubuntu/WSL through the Windows-mounted project directory, with PostgreSQL disabled. Each configuration had two interleaved trials; all processed 541,909 source rows and produced 536,639 Silver rows. Wall-clock medians and aggregate Parquet outputs were:

| Shuffle partitions | Max rows per file | Median elapsed | Parquet files | Parquet bytes | Change vs. 8/0 |
|---:|---:|---:|---:|---:|---:|
| 8 | 0 (unlimited) | 109.7 s | 24 | 46,543,111 | baseline |
| 4 | 0 (unlimited) | 105.3 s | 20 | 45,612,364 | 4.0% faster; 16.7% fewer files; 2.0% fewer bytes |
| 12 | 0 (unlimited) | 107.6 s | 29 | 47,370,907 | 2.0% faster; more files and bytes |
| 8 | 100,000 | 103.8 s | 24 | 46,543,111 | same output layout/size; apparent time gain not attributed to the cap |

Based on this small local result, the default shuffle partition count was changed from 8 to 4. This is a modest measured improvement, not a general Spark tuning rule. The `100,000` row cap was not adopted: it changed neither file count nor total bytes on this dataset, so its faster two-run median is treated as run-to-run variation rather than evidence that the cap helped. The 8 trial details are retained in `data/bench/spark_benchmark_20261031T072759Z.csv`; trial logs are under `logs/bench/`.

Use the median elapsed time per configuration and compare it with the baseline. This is a local benchmark of the 541,909-row UCI workbook on the machine and filesystem used; it does not establish distributed-scale performance. The first Spark startup, Windows-mounted filesystem, CPU load, and background work can affect timings. Repeat the benchmark after material changes to the machine, dataset, or pipeline before drawing further conclusions.

The pipeline caches raw and clean DataFrames because each is consumed by multiple actions. Keep caching only while those repeated actions benefit; avoid adding caches to one-use frames. No repartition is forced by default because a shuffle can cost more than it saves on a single local batch.

## PostgreSQL query plans

Run the plan examples from the project directory:

```powershell
& 'C:\Program Files\PostgreSQL\18\bin\psql.exe' -U postgres -h localhost -p 5432 -d retail -f sql/performance.sql
```

Review actual versus estimated rows, execution time, buffer reads, and whether the existing primary-key or secondary indexes help. Keep `EXPLAIN ANALYZE` on these read-only SELECT statements; it executes the query. Change indexes only after measuring a representative workload, and compare plans and write cost before/after.

### Observed local PostgreSQL verification (2026-10-31)

The configured `retail` database contained 1 batch, 536,639 current line items, 1,716 daily rows, 5,749 product rows, and 38 country rows. The expected fact indexes and Gold primary-key indexes were present. Read-only `EXPLAIN (ANALYZE, BUFFERS)` results showed:

- The daily country/date lookup used `daily_sales_pkey` (26 actual rows; 1.004 ms execution).
- The product lookup used `product_sales_pkey` (4 actual rows; 0.512 ms execution).
- The fact date-range aggregation used `ix_retail_line_daily` through a bitmap index scan, then read 59,969 fact rows and aggregated 171 groups (506.026 ms execution).

These are single local observations and depend on cache state. The fact aggregation remains the costliest of these examples because it reads many rows; this run does not justify another index or a claim of improvement versus a pre-index baseline. No index changes were made during verification.

## Phase 9 acceptance evidence

- A fixture with an additive `Discount` column retains that field through Silver, while revenue logic remains unchanged until explicitly mapped.
- A retained batch can be explicitly reprocessed, and a database backfill replaces current facts and aggregates transactionally.
- Audit records expose batch/run lineage, schema metadata, row counts, outcomes, and duration.
- Spark partition/file settings are configurable and have a repeatable measurement procedure; performance gains are reported only after collecting the measurements.
- PostgreSQL query-plan examples and environment-based credential guidance are included.

## Verification record (2026-10-31)

- The WSL project test suite passed: 11 passed, including the regression that ensures replaying an older batch does not override a newer stable-line correction. The Airflow DAG import test was separately verified in the earlier Phase 8 run. After the local shuffle default was changed to 4, the full pipeline benchmark completed all eight trials successfully; the 4-partition run exercised that selected configuration end to end.
- The audit and Airflow task unit tests passed in the current environment: 3 passed. Windows Spark tests are not treated as authoritative here because this environment has a Hadoop Windows native-library mismatch; the user's WSL run is the relevant full-suite result.
- PostgreSQL row counts, expected indexes, and three actual query plans were inspected against the local `retail` database; observations are recorded above.
- Schema-evolution behavior has a Spark fixture test that asserts the `Discount` value is preserved through Silver, and the full user-run suite included it.
- `scripts/verify_postgres_backfill.py` passed against the isolated WSL database `retail_phase9_verify_20261031084735832401`. It verified the incremental correction, two replays of the older batch, stable two-line/two-batch counts, corrected quantity 3, revenue 12.5000, and Gold row counts 1/2/1. The live Windows `retail` database was not modified.
- The Spark batch-ordering regression test passed as part of the 11-test WSL suite.
