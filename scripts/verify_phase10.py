"""End-to-end acceptance checks against a uniquely named disposable database."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

from verify_postgres_backfill import (connection_args, create_scratch_database,
                                      initialize_schema, run_pipeline)


ROOT = Path(__file__).resolve().parents[1]


def write_batch(path: Path, columns: list[str], rows: list[list[str]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(columns)
        writer.writerows(rows)
    return path


def latest_record(data_dir: Path, batch_id: str) -> dict:
    candidates = []
    for path in (data_dir / "audit").glob("run_*.json"):
        record = json.loads(path.read_text(encoding="utf-8"))
        if record.get("batch_id") == batch_id:
            candidates.append(record)
    if not candidates:
        raise AssertionError(f"No audit record found for batch {batch_id}")
    return max(candidates, key=lambda record: record.get("recorded_at_utc", ""))


def execute_expected_failure(env: dict[str, str], input_path: Path, data_dir: Path) -> dict:
    command = [sys.executable, "-m", "retail_pipeline", "--input", str(input_path)]
    result = subprocess.run(command, cwd=ROOT, env=env, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            check=False)
    if result.returncode == 0:
        raise AssertionError("Injected warehouse failure unexpectedly succeeded")
    from retail_pipeline.audit import fingerprint

    record = latest_record(data_dir, fingerprint(input_path))
    if record.get("status") != "failed" or record.get("stage") != "warehouse":
        raise AssertionError(f"Expected an audited warehouse-stage failure, got {record}")
    print("Injected warehouse transaction failure: correctly failed and audited")
    return record


def state(database: str) -> dict:
    with psycopg2.connect(**connection_args(database)) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*), COUNT(DISTINCT line_id), COALESCE(SUM(line_amount),0)
                FROM retail_line_items
            """)
            fact_count, distinct_lines, fact_revenue = cursor.fetchone()
            cursor.execute("SELECT quantity FROM retail_line_items WHERE invoice_no='1001' AND stock_code='SKU-1'")
            corrected_quantity = cursor.fetchone()[0]
            cursor.execute("SELECT quantity FROM retail_line_items WHERE invoice_no='1000' AND stock_code='SKU-0'")
            untouched_quantity = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM pipeline_batches")
            batch_count = cursor.fetchone()[0]
            metrics = {}
            for table in ("daily_sales", "product_sales", "country_sales"):
                cursor.execute(f"SELECT COUNT(*), COALESCE(SUM(net_revenue),0) FROM {table}")
                metrics[table] = cursor.fetchone()
    return {
        "facts": fact_count,
        "distinct_lines": distinct_lines,
        "fact_revenue": fact_revenue,
        "corrected_quantity": corrected_quantity,
        "untouched_quantity": untouched_quantity,
        "batches": batch_count,
        "daily_rows": metrics["daily_sales"][0],
        "daily_revenue": metrics["daily_sales"][1],
        "product_rows": metrics["product_sales"][0],
        "product_revenue": metrics["product_sales"][1],
        "country_rows": metrics["country_sales"][0],
        "country_revenue": metrics["country_sales"][1],
    }


def assert_state(database: str, *, lines: int, revenue: str,
                 corrected_quantity: int, batches: int, products: int,
                 label: str) -> None:
    observed = state(database)
    expected_total = Decimal(revenue)
    expected = {
        "facts": lines,
        "distinct_lines": lines,
        "fact_revenue": expected_total,
        "corrected_quantity": corrected_quantity,
        "untouched_quantity": 1,
        "batches": batches,
        "daily_rows": 1,
        "daily_revenue": expected_total,
        "product_rows": products,
        "product_revenue": expected_total,
        "country_rows": 1,
        "country_revenue": expected_total,
    }
    if observed != expected:
        raise AssertionError(f"{label}: expected {expected}; got {observed}")
    print(f"{label}: PASS — {lines} unique facts, revenue {revenue}, {batches} batches; Gold totals reconcile")


def install_failure_trigger(database: str) -> None:
    with psycopg2.connect(**connection_args(database)) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE FUNCTION phase10_injected_failure() RETURNS trigger AS $$
                BEGIN
                    IF NEW.invoice_no = '1003' THEN
                        RAISE EXCEPTION 'intentional Phase 10 failure injection';
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql
            """)
            cursor.execute("""
                CREATE TRIGGER phase10_injected_failure_trigger
                BEFORE INSERT OR UPDATE ON retail_line_items
                FOR EACH ROW EXECUTE FUNCTION phase10_injected_failure()
            """)


def remove_failure_trigger(database: str) -> None:
    with psycopg2.connect(**connection_args(database)) as conn:
        with conn.cursor() as cursor:
            cursor.execute("DROP TRIGGER phase10_injected_failure_trigger ON retail_line_items")
            cursor.execute("DROP FUNCTION phase10_injected_failure()")


def main() -> int:
    load_dotenv(ROOT / ".env")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    database = f"retail_phase10_verify_{stamp}"
    run_root = ROOT / "data" / "bench" / f"phase10_{stamp}"
    data_dir = run_root / "data"
    input_dir = run_root / "input"
    log_dir = ROOT / "logs" / "bench" / f"phase10_{stamp}"
    base_columns = ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate",
                    "UnitPrice", "CustomerID", "Country", "LineID"]
    batch1 = write_batch(input_dir / "batch_1.csv", base_columns, [
        ["1000", "SKU-0", "Marker", "1", "2011-01-01 09:00:00", "1.00", "6", "United Kingdom", "line-0"],
        ["1001", "SKU-1", "Mug", "2", "2011-01-01 10:00:00", "3.50", "7", "United Kingdom", "line-1"],
    ])
    batch2 = write_batch(input_dir / "batch_2.csv", base_columns, [
        ["1001", "SKU-1", "Mug", "3", "2011-01-01 10:00:00", "3.50", "7", "United Kingdom", "line-1"],
        ["1002", "SKU-2", "Plate", "1", "2011-01-01 11:00:00", "2.00", "8", "United Kingdom", "line-2"],
    ])
    batch3 = write_batch(input_dir / "batch_3.csv", base_columns, [
        ["1003", "SKU-3", "Book", "2", "2011-01-01 12:00:00", "4.00", "9", "United Kingdom", "line-3"],
    ])
    batch4 = write_batch(input_dir / "batch_4_invalid.csv", base_columns, [
        ["1004", "SKU-4", "Notebook", "1", "2011-01-01 13:00:00", "5.00", "10", "United Kingdom", "line-4"],
        ["1005", "SKU-5", "Bad quantity", "many", "2011-01-01 14:00:00", "6.00", "11", "United Kingdom", "bad-qty"],
        ["1006", "SKU-6", "Missing country", "1", "2011-01-01 15:00:00", "2.00", "12", "", "bad-country"],
    ])
    batch5_columns = [*base_columns, "Discount"]
    batch5 = write_batch(input_dir / "batch_5_schema_change.csv", batch5_columns, [
        ["1007", "SKU-7", "Pen", "1", "2011-01-01 16:00:00", "8.00", "13", "United Kingdom", "line-5", "0.10"],
    ])

    print(f"Creating isolated verification database: {database}")
    print("The configured retail database will not be used or modified.")
    create_scratch_database(database)
    initialize_schema(database)

    env = os.environ.copy()
    env.update({"DATA_DIR": str(data_dir), "LOG_DIR": str(log_dir),
                "WAREHOUSE_ENABLED": "true", "PGDATABASE": database})

    from retail_pipeline.audit import fingerprint

    print("\nA. Fresh pipeline run")
    run_pipeline(env, "--input", str(batch1))
    first_id = fingerprint(batch1)
    first_audit = latest_record(data_dir, first_id)
    if (first_audit.get("source"), first_audit.get("bronze"), first_audit.get("accepted"),
            first_audit.get("quarantine_parquet"), first_audit.get("silver_parquet"),
            first_audit.get("gold_rows")) != (2, 2, 2, 0, 2,
                {"daily_sales": 1, "product_sales": 2, "country_sales": 1}):
        raise AssertionError(f"Fresh-layer persisted counts are wrong: {first_audit}")
    assert_state(database, lines=2, revenue="8.0000", corrected_quantity=2,
                 batches=1, products=2, label="Fresh warehouse and Gold")

    print("\nB. Same batch forced twice")
    run_pipeline(env, "--input", str(batch1), "--force")
    replay_audit = latest_record(data_dir, first_id)
    if replay_audit.get("status") != "skipped":
        raise AssertionError(f"Expected batch-ledger skip on forced repeat: {replay_audit}")
    assert_state(database, lines=2, revenue="8.0000", corrected_quantity=2,
                 batches=1, products=2, label="Forced duplicate batch")

    print("\nC. New batch corrects one stable line and adds one new line")
    run_pipeline(env, "--input", str(batch2))
    correction_audit = latest_record(data_dir, fingerprint(batch2))
    if (correction_audit.get("source"), correction_audit.get("accepted"),
            correction_audit.get("silver"), correction_audit.get("quarantined")) != (2, 2, 2, 0):
        raise AssertionError(f"Unexpected incoming-batch processing counts: {correction_audit}")
    assert_state(database, lines=3, revenue="13.5000", corrected_quantity=3,
                 batches=2, products=3, label="Incremental correction; untouched earlier line retained")

    print("\nD. Inject a failure inside the PostgreSQL fact transaction")
    before_failure = state(database)
    install_failure_trigger(database)
    failed_audit = execute_expected_failure(env, batch3, data_dir)
    if failed_audit.get("counts", {}).get("silver") != 1:
        raise AssertionError(f"Failure did not occur after Silver was prepared: {failed_audit}")
    if state(database) != before_failure:
        raise AssertionError("Warehouse state changed despite the injected transaction failure")
    remove_failure_trigger(database)
    assert_state(database, lines=3, revenue="13.5000", corrected_quantity=3,
                 batches=2, products=3, label="After rollback")
    print("Retrying the exact failed batch")
    run_pipeline(env, "--input", str(batch3))
    assert_state(database, lines=4, revenue="21.5000", corrected_quantity=3,
                 batches=3, products=4, label="Recovered batch")

    print("\nE. Invalid rows are rejected and quarantined")
    run_pipeline(env, "--input", str(batch4))
    invalid_audit = latest_record(data_dir, fingerprint(batch4))
    if (invalid_audit.get("source"), invalid_audit.get("accepted"),
            invalid_audit.get("quarantined"), invalid_audit.get("quarantine_parquet")) != (3, 1, 2, 2):
        raise AssertionError(f"Unexpected invalid-row metrics: {invalid_audit}")
    reasons = invalid_audit.get("rejection_reasons", {})
    if reasons.get("invalid_quantity") != 1 or reasons.get("missing_country") != 1:
        raise AssertionError(f"Invalid rows were not assigned expected reason codes: {reasons}")
    assert_state(database, lines=5, revenue="26.5000", corrected_quantity=3,
                 batches=4, products=5, label="After quarantining invalid rows")

    print("\nF. Additive source schema is visible and revenue stays unchanged by Discount")
    run_pipeline(env, "--input", str(batch5))
    schema_audit = latest_record(data_dir, fingerprint(batch5))
    if schema_audit.get("source_extra_columns") != "Discount":
        raise AssertionError(f"Additive Discount column was not recorded: {schema_audit}")
    assert_state(database, lines=6, revenue="34.5000", corrected_quantity=3,
                 batches=5, products=6, label="After additive-schema batch")

    print("\nG. Replay the historical batch twice")
    for replay_number in (1, 2):
        run_pipeline(env, "--backfill-batch", first_id)
        assert_state(database, lines=6, revenue="34.5000", corrected_quantity=3,
                     batches=5, products=6, label=f"Historical replay {replay_number}")

    print("\nPHASE 10 END-TO-END VERIFICATION PASSED")
    print(f"Disposable PostgreSQL database retained for review: {database}")
    print(f"Fixtures, Parquet outputs, and audit records retained under: {run_root}")
    print("No automatic cleanup is performed. The configured retail database was not used.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
