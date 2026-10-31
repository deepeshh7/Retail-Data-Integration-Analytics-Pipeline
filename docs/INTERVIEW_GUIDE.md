# Retail Data Pipeline: Interview Guide

## 60-second project pitch

I built a local batch ELT pipeline for the UCI Online Retail dataset using Python and PySpark. It reads an Excel or CSV source, keeps a raw copy, adds ingestion metadata, validates rows, and separates rejected rows into a quarantine layer with reason codes. Clean, deduplicated input batches are written as Silver Parquet. In warehouse mode, PostgreSQL refreshes only the daily, product, and country aggregate groups affected by each batch; compact Gold Parquet files mirror those tables. I added a SHA-256 batch ID and completion manifest. In warehouse mode, a transactional batch ledger prevents duplicate application; a current line table supports upserts and lets the job refresh only affected aggregate keys. This is a small local portfolio project, not a claim of distributed-scale performance.

## Walkthrough: follow one row

1. **Input and raw:** `ingest.py` reads the source and, for XLSX, streams it to CSV so Spark can process it without loading the whole workbook into a Python list. The original workbook is copied under `data/raw/`.
2. **Bronze:** `runner.py` adds source filename, SHA-256 source fingerprint, and ingestion timestamp, then writes the source fields as strings plus metadata to Parquet; type casting happens in the standardization step.
3. **Quality and quarantine:** `quality.py` standardizes names and types, then checks required fields, timestamps, quantity, and unit price. Rejected rows retain one or more reason codes under `data/quarantine/`.
4. **Silver:** `transform.py` removes exact duplicate business records, calculates `line_amount`, derives `sale_date`, and creates a stable `line_id`. The current warehouse state is deduplicated/upserted by that ID; Silver Parquet preserves each processed batch.
5. **Gold:** In PostgreSQL mode, the database recomputes only affected date, product, and country groups from current line state. The compact current aggregates are copied to Parquet under `data/gold/`.
6. **Warehouse and audit:** When enabled, `warehouse.py` writes batches to PostgreSQL using `INSERT ... ON CONFLICT ... DO UPDATE` within a transaction. `audit.py` tracks completed source fingerprints and writes a JSON run record; PostgreSQL tracks applied batch IDs transactionally.

## Suggested interview demo order

1. State the problem and show the architecture diagram in `README.md`.
2. Explain the trust boundary: raw/Bronze preserves input, Silver is typed and validated, quarantine retains rejects, and Gold is aggregated.
3. Trace one event through `ingest.py`, `quality.py`, `transform.py`, and `warehouse.py`, calling out the batch fingerprint, stable line key, and lineage columns.
4. Show the disposable Phase 10 acceptance runner. It proves same-batch idempotency, a stable-ID correction, rollback/retry, invalid-row quarantine, additive schema handling, and historical replay without touching the real `retail` database.
5. Show the Gold query and compare its total with fact revenue. Then show the measured 4 versus 8 shuffle-partition benchmark, describing it as a small local result.
6. Close with the limitations below and explain what would change for a cloud or larger-volume deployment.

## Results from the successful local run

These values are from the run output and the PostgreSQL count query, not performance benchmarks:

| Measure | Observed result |
| --- | ---: |
| Source rows | 541,909 |
| Rows accepted by validation | 541,907 |
| Rows quarantined | 2 |
| Silver rows after deduplication | 536,639 |
| `daily_sales` rows in PostgreSQL | 1,716 |
| `product_sales` rows in PostgreSQL | 5,749 |
| `country_sales` rows in PostgreSQL | 38 |

The difference between accepted and Silver counts represents records removed by deduplication in this run. Do not describe the two quarantined records' causes unless you inspect their reason codes.

## Revenue definitions

- `line_amount` is `quantity * unit_price`.
- `net_revenue` sums `line_amount`, so negative-quantity rows reduce net revenue.
- `gross_revenue` sums line amounts only for positive-quantity rows.
- `return_value` sums negative-quantity line amounts and is therefore negative or zero in this implementation. It is not an absolute-value measure.
- Cancellation flags are derived from invoice numbers beginning with `C`; the pipeline keeps those rows and does not separately exclude them from the aggregates. Explain that as a modeling decision that could be refined if the business defines cancellation handling differently.

## Likely questions and clear answers

**Why PySpark and Parquet?**

PySpark expresses transformations as DataFrame operations and gives a path to cluster execution if scale justifies it. Parquet is compressed and column-oriented, so downstream jobs can read selected columns without reparsing a text file. CSV remains useful at the source boundary, but it does not preserve types and is less efficient for repeated analytical reads. Here Spark runs locally; the benchmark only supports a modest single-machine result, not a distributed performance claim.

**What is a data lake, a warehouse, and a lakehouse?**

A data lake stores source and curated files in low-cost object or file storage, often with flexible schemas. A warehouse organizes modeled, governed tables for SQL analytics. A lakehouse uses table formats and metadata on lake storage to add warehouse-like transactions, schema enforcement, and reliable table management. This project has a local Parquet lake and a PostgreSQL warehouse; it is not a lakehouse platform because plain Parquet folders do not supply table transactions or a catalog.

**Why Bronze, Silver, and Gold?**

Bronze preserves what arrived plus lineage, so I can trace and replay a batch. Silver applies types, quality rules, stable keys, and deduplication. Gold is aggregated for business queries. The boundaries make it possible to inspect rejected input and reprocess a source without confusing raw data with trusted analytics.

**How is the pipeline idempotent?**

It hashes the source file and records completed fingerprints. An unchanged completed input is skipped unless `--force` is supplied. For UCI, a hash of the complete normalized event preserves distinct lines and exact retries. If a source provides `LineID`, it becomes the stable update key. PostgreSQL records the batch ID, upserts current line state, and refreshes affected Gold groups in one transaction. The JSON file manifest is still outside the PostgreSQL transaction; the database ledger is the authoritative retry guard for warehouse mode.

**What happens if a pipeline fails?**

Before the warehouse transaction commits, PostgreSQL rolls back its fact upserts, Gold refreshes, and ledger insert together. The run writes a failure audit with the stage and error; after the cause is fixed, the same source can be retried. If the database commits but writing Gold Parquet or the JSON manifest later fails, the batch ledger records the commit. A forced retry checks the ledger and refreshes Gold Parquet from PostgreSQL rather than applying the batch again. Bronze/Silver files can remain after a failure; the database transaction is the trusted publication boundary for warehouse mode.

**How do you handle bad data?**

Validation splits accepted and rejected DataFrames. Rejected rows are preserved in quarantine with reason codes, so failures can be inspected rather than silently dropped.

**How are returns handled?**

Negative quantities are retained as separate events. They lower net revenue and are counted in the negative `return_value` measure. Gross revenue sums only positive-quantity lines. Cancellation flags are preserved for downstream analysis. A late event updates its date/product/country groups. For updates, use a source-provided `LineID`; when the UCI source has no line ID, represent corrections as offsetting events so history is not guessed or overwritten.

**How does incremental loading work?**

The source fingerprint identifies a batch. The pipeline stages only its clean rows, then uses `line_id` as the upsert key. It captures the old and new date/product/country group keys, updates current lines, and recomputes only those Gold groups from PostgreSQL. Late-arriving records naturally target their event-date groups. A full historical correction uses the explicit batch backfill path, which rebuilds facts and Gold transactionally from retained Silver batches.

**How would you scale this architecture?**

First measure data size, file counts, skew, and warehouse write time. Move raw and Parquet layers to object storage, partition by a useful date or batch field without creating many tiny files, and run Spark on a managed cluster only if volume needs it. Replace row-wise driver staging with bulk load into a database staging table and database-side merge. Add a catalog, retention policy, alerting, and capacity monitoring. The current local `local[*]` run and 541k-row dataset are a learning baseline, not evidence of cluster throughput.

**How would you move it to AWS or Azure?**

On AWS I would use S3 for the lake, Glue Data Catalog, and Glue or EMR for Spark; on Azure I would use ADLS Gen2, a catalog such as Unity Catalog or an equivalent, and Databricks/Synapse Spark. Airflow could be hosted or replaced by a managed orchestrator. Use IAM roles or managed identities rather than static keys, a cloud secrets manager for database credentials, private networking, and a managed warehouse target such as Aurora/Redshift or Azure Database for PostgreSQL/Synapse. This repository has no cloud deployment; those are migration options, not implemented features.

**What are the current limitations?**

Spark runs locally against a modest dataset; the PostgreSQL path streams records through the Python driver and should switch to bulk staging for larger volumes. Plain Parquet directories do not provide ACID table transactions or automatic schema evolution. The UCI source lacks a stable row ID, so a changed row cannot be distinguished from a new event without a source line key. PostgreSQL and Parquet are not committed in one distributed transaction; the ledger and retry recovery protect warehouse correctness, while Parquet snapshots can be refreshed. There is no cloud deployment, secrets manager, catalog, external alerting, or production-grade access-control setup. The Phase 10 acceptance scenarios passed on 2026-10-31 against a disposable PostgreSQL database; rerun them after material pipeline or warehouse changes.

**What would you improve before production?**

I would move storage and compute to a managed platform only after measuring requirements, use an ACID table format for the lake, publish snapshots atomically, add data-quality thresholds and alerting, bulk-stage warehouse updates, and define data retention and access controls. I would also confirm the business definitions for returns and cancellations with stakeholders.

## Demo commands

Run these from the project root with `.venv` active. The dataset and PostgreSQL connection must already be configured.

```powershell
python -m pytest
python -m retail_pipeline --input data/input/online_retail.xlsx --force
& 'C:\Program Files\PostgreSQL\18\bin\psql.exe' -U postgres -h localhost -p 5432 -d retail -c "SELECT 'daily_sales' AS table_name, COUNT(*) AS row_count FROM daily_sales UNION ALL SELECT 'product_sales', COUNT(*) FROM product_sales UNION ALL SELECT 'country_sales', COUNT(*) FROM country_sales;"
```

For the isolated final acceptance run, use the WSL instructions in [`PHASE_10_VALIDATION.md`](PHASE_10_VALIDATION.md). It creates a disposable PostgreSQL database and prints its name; do not substitute the production/demo `retail` database.

Never show or paste `.env` contents during an interview. The data workbook and generated Parquet are local files and are not committed to Git.
