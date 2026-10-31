# Phase 5 — Pipeline Audit and Baseline

Audit date: 2026-10-28

## Scope and evidence

This is a code and artifact audit of the existing local pipeline. The project was not rewritten.

Evidence used:

- Source, transformation, orchestration, warehouse, and audit modules under `src/retail_pipeline/`.
- PostgreSQL DDL and example queries under `sql/`.
- Existing test files under `tests/`.
- The latest saved run record at `data/audit/run_20261028T123455_158492Z.json`.
- The successful pipeline log and `psql` output supplied by the user in this conversation.

The saved audit record contains status `completed`, fingerprint `43465a06f2ccf7c8b5bd2892bc7defb52f97487934fe93b16ae4c3936424676d`, source 541,909, accepted 541,907, quarantined 2, and Silver 536,639. The user's PostgreSQL query returned 1,716 rows in `daily_sales`, 5,749 in `product_sales`, and 38 in `country_sales`. This environment could not independently open a PostgreSQL connection; the database counts above are based on the user's successful `psql` output.

## Flow verification

| Stage | Implementation | Verification / result |
| --- | --- | --- |
| Input and raw | `ingest.py` accepts XLSX/XLSM and CSV, checks expected columns, streams workbooks to a sibling CSV, and copies the source under a SHA-256 fingerprint directory. | User run read 541,909 rows from the supplied UCI workbook. XLSX conversion has a timestamp-based CSV cache. |
| Bronze | `runner.py` adds source filename, fingerprint, and ingestion timestamp, then writes one fingerprinted Parquet directory. Source fields are read with `inferSchema=False`, so they are strings in Bronze; standardization performs casts later. | Code path and generated Bronze files inspected. The previous phrase “typed source rows” was inaccurate and has been corrected in the README. |
| Quality / quarantine | `quality.py` casts fields and splits rows based on missing invoice/stock/country, invalid timestamp, null/zero quantity, and null/negative price. Reason codes are attached to rejected rows. | User run: 541,907 accepted and 2 quarantined. Existing Spark test covers zero quantity rejection and preservation of a negative-quantity cancellation row. |
| Silver | `transform.py` drops duplicate business-field combinations, calculates `line_amount`, `sale_date`, and SHA-256 `line_id`. `runner.py` unions new rows with existing Silver and deduplicates by `line_id`. | User run: 536,639 Silver records. Accepted-minus-Silver is 5,268 rows removed as duplicates. |
| Gold Parquet | `gold()` groups Silver into daily/country, product, and country aggregates. `runner.py` overwrites each Gold output directory. | User's successful log reports all three tables written. Gold aggregate row counts align with the PostgreSQL counts for the reported baseline. |
| PostgreSQL | `schema.sql` defines all three tables and matching primary keys; `warehouse.py` inserts selected Gold columns in batches of 1,000 with `ON CONFLICT ... DO UPDATE` through one psycopg2 transaction. | User's `psql` query confirmed 1,716 daily, 5,749 product, and 38 country rows. The table/column names and loader mappings match the DDL. |
| Audit / idempotency | SHA-256 source fingerprints are recorded in a JSON manifest after successful processing; each non-skipped returned run gets a separate JSON record. | Latest saved JSON record reports `completed` with counts matching the user's run. An unchanged completed fingerprint is skipped unless `--force` is used. |

## Existing tests, logging, validation, and failures

- Tests: `test_quality.py` has two Spark tests for invalid quantity, returns, cancellation flag, and aggregate net revenue. `test_audit.py` has one test for deterministic fingerprints and repeatable manifest updates.
- User-provided baseline: the user earlier ran `python -m pytest` successfully with **3 passed**.
- Audit-session test attempt: **1 passed, 2 errors**. The non-Spark audit test passed. Both Spark tests failed during Java gateway startup because this execution environment denied the local socket (`WinError 10013`); this is an environment restriction, not evidence of a transformation assertion failure. The full pipeline had previously run successfully in the user's terminal.
- Logging: the CLI writes INFO-level messages to both `logs/pipeline.log` and the console. Stages and row counts are logged; a successful run also gets a JSON audit record.
- Validation: checks are implemented in Spark expressions; invalid records are retained in a quarantine Parquet output with reason codes. There is no threshold that fails the batch when quarantine volume is high.
- Error handling: `runner.py` ensures Spark is stopped in `finally`, and PostgreSQL's connection context rolls back an errored transaction. There is no retry strategy or failure audit record. Exceptions from the CLI are not translated into a concise user-facing error.

## Baseline

| Measure | Baseline |
| --- | ---: |
| Source rows | 541,909 |
| Accepted rows | 541,907 |
| Quarantined rows | 2 |
| Silver after deduplication | 536,639 |
| Duplicate rows removed (`accepted - silver`) | 5,268 |
| PostgreSQL `daily_sales` | 1,716 |
| PostgreSQL `product_sales` | 5,749 |
| PostgreSQL `country_sales` | 38 |

These are observed results from the supplied workbook and the run on 2026-10-28, not throughput or scalability benchmarks. Spark ran locally on Windows. The dataset workbook and generated Parquet are local, ignored files.

## Gap analysis

### High priority before treating this as robust/re-runnable

1. **Layer writes are not atomic.** Silver replacement removes the prior directory before renaming staging; a process interruption can leave Silver missing. Gold tables are overwritten one at a time, so a failure can leave Gold layers from different runs. Publish through versioned/staging paths and swap a single run pointer or add recovery logic.
2. **Warehouse upserts do not remove disappeared keys.** If corrected source data removes a product/country/day key, `ON CONFLICT DO UPDATE` never deletes its old PostgreSQL row. The current initial dataset counts are verified, but key deletion/correction semantics are not fully synchronized. Use transactional stage-and-replace or merge with a run identifier when correction semantics are required.
3. **Manifest and output commits are not coordinated.** The manifest is written only after the warehouse step, which is good for avoiding a false completed marker, but files may already have changed if the warehouse write fails. Manifest JSON writes are also not atomic or safe for concurrent pipeline processes.
4. **Fixed Silver scratch paths and concurrency.** `silver_incoming` and `silver_staging` are shared fixed directories; overlapping runs can overwrite or remove each other's state.

### Medium priority for clearer data contracts and operations

5. **Deduplication assumes repeated business fields mean duplicate lines.** There is no source line identifier; two genuinely separate, identical lines in one invoice could be collapsed. The stable ID also concatenates fields with `||`, which can theoretically collide if source values contain that delimiter. Define a source record key or document/test this assumption.
6. **Product natural key includes description.** A description correction for a stock code creates another PostgreSQL product row instead of updating one product. Confirm whether the key should be only `stock_code` or whether descriptions intentionally define variants.
7. **Audit covers successful returned runs only.** Skipped runs are not written to the per-run audit, and failures do not get a failure record. Add status, stage, timestamps, and exception summaries if operational traceability is a goal.
8. **Quality rules encode assumptions.** `CustomerID` and `Description` may be null without rejection; zero unit price is accepted; negative quantities are retained; cancellation flags do not filter rows. These are explicit current behaviors that should be confirmed against business requirements.
9. **Quarantine monitoring is limited.** Reason codes exist, but there are no quality thresholds, alerts, or tests for all individual reason codes and malformed schemas.
10. **Dry run still writes local data layers.** `--dry-run` skips only the PostgreSQL write and completion manifest; it still copies raw data and writes Bronze, quarantine, Silver, Gold, and the success audit JSON. Consider a clearer name/help message or a true no-output validation mode.

### Test coverage gaps

- No direct tests for XLSX-to-CSV conversion, required-column checks, or CSV header edge cases.
- No runner tests for first run, repeat skip, `--force`, changed input, interrupted writes, or dry-run side effects.
- No PostgreSQL tests for schema compatibility, batch boundaries, transaction rollback, or stale-key removal.
- No tests for every validation reason or the exact daily/product/country aggregate values.
- No SQL integration test against a clean database.

## Conclusion

The end-to-end path is implemented and the recorded successful run, saved audit record, and user-provided PostgreSQL counts establish a working baseline for this dataset. The implementation is suitable as a local interview/portfolio project. Its main limits are crash recovery and concurrent-run safety, warehouse synchronization when aggregate keys disappear, assumptions in deduplication/business rules, and incomplete failure auditing/test coverage. No feature additions or pipeline rewrites were made in this audit.
