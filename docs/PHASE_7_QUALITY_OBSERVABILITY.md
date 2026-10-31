# Phase 7 — Data Quality and Observability

## Implemented checks

- Required invoice number, stock code, invoice timestamp, quantity, unit price, and country are validated after casting/standardization.
- Invalid numeric casts, zero quantity, negative unit price, line-amount overflow, invalid dates, blank required fields, and conflicting versions of a supplied `LineID` are quarantined with reason codes. Negative quantity remains a valid return event. Customer ID and description remain optional; missing descriptions become `<UNKNOWN>`.
- The run checks `source rows = accepted rows + quarantined rows`, `Silver batch rows <= accepted rows`, and reads back Bronze, quarantine, Silver batch, and Gold Parquet to verify the written row counts.
- Gold net revenue is compared across daily, product, and country aggregates. The local full-rebuild path also compares all Gold totals with current Silver. In warehouse mode, PostgreSQL compares fact and Gold revenue for impacted keys inside the same transaction; a mismatch rolls back the batch.
- Each run has a UUID run ID. Bronze and Silver rows carry it, `logs/pipeline.log` is JSON Lines with run/batch/stage context, and `data/audit/run_*.json` contains status, timings, counts, rejection-reason totals, Gold row counts/revenue, and failures.
- Audit and completion JSON are written through a same-directory temporary file and atomic replace. PostgreSQL batch/fact/Gold updates remain transactional. A failure records the stage and message; a committed warehouse batch can recover Gold Parquet on retry without replaying source rows.

## Invalid-record acceptance test

`tests/test_quality.py::test_invalid_records_are_quarantined_with_types_and_reason_codes` creates one valid row and nine deliberately invalid rows: missing invoice, missing stock code, invalid date, nonnumeric quantity, zero quantity, nonnumeric unit price, negative unit price, missing country, and malformed nonblank customer ID. It asserts that only the valid row is accepted, all nine rejected rows are persisted to Parquet, the expected reason codes are present, and output types are cast to `BIGINT` / `DECIMAL`.

Run from the project root with `.venv` active:

```powershell
python -m pytest
```

The Spark tests require Java, a normal local Python/JVM socket, and on Windows matching Hadoop helper binaries (`winutils.exe` and `hadoop.dll`) discoverable through `HADOOP_HOME`. A successful run should include the quarantine test and report all tests passing. The current raw workbook is not modified by the test fixture.

## Run records and failure recovery

A successful or failed record includes `run_id`, `batch_id`, status, stage, duration, counts, and a failure type/message when applicable. Inspect the latest JSON file in `data/audit/` and the JSON event lines in `logs/pipeline.log`. If a run fails before commit, the PostgreSQL transaction rolls back and retrying the same input is safe. If PostgreSQL commits but a later Parquet/manifest step fails, a forced retry detects the committed batch and refreshes Gold Parquet from the warehouse.

No passwords or environment values are written into run logs or audit JSON.
