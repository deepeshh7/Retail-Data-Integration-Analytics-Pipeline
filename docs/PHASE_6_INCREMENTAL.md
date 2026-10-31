# Phase 6 — Incremental Loading and Idempotency

## Design

- A source file's SHA-256 fingerprint is its batch ID. The existing JSON manifest skips completed files. PostgreSQL also records `pipeline_batches.batch_id` in the same database transaction as its line changes and Gold refresh; this ledger protects forced retries and recovery after a Parquet/manifest failure.
- In warehouse-enabled mode, each input batch is transformed once and written to `data/silver_batches/source_fingerprint=<sha256>/`. The PostgreSQL `retail_line_items` table stores current line state. Incoming lines are staged and upserted by `line_id`; the existing fact table is not loaded back into Spark for each batch.
- The first incremental run can seed `retail_line_items` from the prior `data/silver` snapshot and rebuild the Gold tables once. Subsequent batches refresh only affected daily `(sale_date,country)`, product `(stock_code,description)`, and country keys. Indexes support those group lookups. Distinct invoice/customer counts are recalculated from current line state for affected groups rather than added as deltas.
- The transaction captures old and new group keys before each upsert, applies line changes, deletes/rebuilds those Gold groups, and inserts the batch ledger row atomically. A correction with a supplied stable ID can move a row between dates/products/countries without leaving old aggregates behind.
- Gold tables in PostgreSQL are the current queryable aggregates. Their Parquet copies are small snapshots refreshed from PostgreSQL after the transaction. Without PostgreSQL, the local fallback combines Silver batches and performs a full Gold rebuild.

## Identity and business assumptions

UCI Online Retail does not include a stable transaction-line ID. Using only invoice number and stock code would collapse distinct rows; the source scan found repeated invoice/stock pairs with different timestamps and other attributes. Therefore, UCI rows use a SHA-256 hash of the complete normalized event payload. Exact repeated events from the same or a different batch have one current fact row. A changed quantity or price without a source line ID is treated as a new event, not guessed to be an update. Represent a correction as a compensating event plus its replacement so the original history is retained.

For another source, the reader accepts `LineID`, `line_id`, or `LineItemID`. That source-provided ID is hashed and used as the stable update key. If one batch contains conflicting payloads for the same supplied ID, all versions are quarantined as `duplicate_line_id_conflict`. A return remains a separate negative-quantity event; a late-arriving event recomputes the affected groups using its event date.

## Applying this change to the existing local project

For a project that has not migrated yet, the existing source fingerprint is in the JSON manifest, so use `--force` once while PostgreSQL is running. That first run seeds current line state from the old Silver Parquet and records the batch ID. A repeated forced run is then skipped by the committed batch ledger before Spark starts. A normal rerun without `--force` is skipped by the file manifest. If PostgreSQL committed but the manifest did not, a forced retry synchronizes Gold Parquet from PostgreSQL without reloading the source.

Use `psql` to compare counts before and after the second forced run:

```sql
SELECT
  (SELECT COUNT(*) FROM pipeline_batches) AS batches,
  (SELECT COUNT(*) FROM retail_line_items) AS current_lines,
  (SELECT COUNT(*) FROM daily_sales) AS daily_rows,
  (SELECT COUNT(*) FROM product_sales) AS product_rows,
  (SELECT COUNT(*) FROM country_sales) AS country_rows;
```

Acceptance check passed on 2026-10-28. The first run logged `Warehouse batch applied`; the second same-fingerprint forced run logged `already applied; no-op`. The PostgreSQL query returned 1 batch, 536,639 current lines, 1,716 daily rows, 5,749 product rows, and 38 country rows. These counts confirm the retry did not duplicate the batch or change the aggregates.

## Validation and remaining limits

- Added tests cover stable updates with a supplied line ID, distinct fallback event IDs when event fields change, and quarantine of conflicting supplied IDs.
- `compileall` completed. The non-Spark audit test passed. Spark tests could not start in this Codex execution environment because Windows denied PySpark's local Java socket (`WinError 10013`). The PostgreSQL transaction was exercised successfully by the user in their normal PowerShell terminal; the acceptance check above passed.
- This uses a local PostgreSQL warehouse and streams the batch through the driver in 1,000-row chunks. It is suitable for this project dataset, not a distributed-scale throughput claim.
- The workbook has no immutable line ID, so updates to a UCI row cannot be distinguished reliably from a new event; use explicit adjustment events. The JSON manifest and Parquet writes are not part of the PostgreSQL transaction. PostgreSQL state remains retry-safe, and a missing manifest triggers a compact Gold Parquet recovery from PostgreSQL. Overlapping different batches can still race on Parquet outputs.
- This design is append/change-batch oriented. A file that omits prior rows does not delete those rows; explicit deletes require tombstone/retraction records.
