"""Safely verify incremental updates and historical backfill in a new database."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql


ROOT = Path(__file__).resolve().parents[1]


def connection_args(database: str) -> dict:
    return {
        "host": os.getenv("PGHOST", "localhost"),
        "port": int(os.getenv("PGPORT", "5432")),
        "dbname": database,
        "user": os.getenv("PGUSER", "postgres"),
        "password": os.getenv("PGPASSWORD", ""),
    }


def create_scratch_database(database: str) -> None:
    # Use a unique database and refuse to reuse one; never touch configured retail.
    conn = psycopg2.connect(**connection_args("postgres"))
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname=%s", (database,))
            if cursor.fetchone():
                raise RuntimeError(f"Refusing to reuse existing scratch database {database}")
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    finally:
        conn.close()


def initialize_schema(database: str) -> None:
    schema_sql = (ROOT / "sql" / "schema.sql").read_text(encoding="utf-8")
    with psycopg2.connect(**connection_args(database)) as conn:
        with conn.cursor() as cursor:
            for statement in schema_sql.split(";"):
                if statement.strip():
                    cursor.execute(statement)


def write_fixtures(input_dir: Path) -> tuple[Path, Path]:
    input_dir.mkdir(parents=True, exist_ok=False)
    columns = "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country,LineID\n"
    first = input_dir / "batch_1.csv"
    second = input_dir / "batch_2.csv"
    first.write_text(
        columns + "1001,SKU-1,Mug,2,2011-01-01 10:00:00,3.50,7,United Kingdom,line-1\n",
        encoding="utf-8",
    )
    second.write_text(
        columns
        + "1001,SKU-1,Mug,3,2011-01-01 10:00:00,3.50,7,United Kingdom,line-1\n"
        + "1002,SKU-2,Plate,1,2011-01-01 11:00:00,2.00,8,United Kingdom,line-2\n",
        encoding="utf-8",
    )
    return first, second


def run_pipeline(env: dict[str, str], *args: str) -> None:
    command = [sys.executable, "-m", "retail_pipeline", *args]
    result = subprocess.run(command, cwd=ROOT, env=env, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            check=False)
    if result.returncode:
        raise RuntimeError(
            f"Pipeline command failed: {' '.join(command)}\n"
            f"Exit code: {result.returncode}\n{result.stdout[-6000:]}"
        )
    print(result.stdout.rstrip())


def snapshot(database: str) -> tuple:
    with psycopg2.connect(**connection_args(database)) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*), COUNT(DISTINCT line_id), SUM(line_amount) FROM retail_line_items")
            fact_rows, unique_lines, revenue = cursor.fetchone()
            cursor.execute("SELECT quantity FROM retail_line_items WHERE invoice_no='1001' AND stock_code='SKU-1'")
            corrected_quantity = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM pipeline_batches")
            batches = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM daily_sales")
            daily_rows = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM product_sales")
            product_rows = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM country_sales")
            country_rows = cursor.fetchone()[0]
    return (fact_rows, unique_lines, revenue, corrected_quantity, batches,
            daily_rows, product_rows, country_rows)


def assert_expected(actual: tuple, stage: str) -> None:
    expected = (2, 2, Decimal("12.5000"), 3, 2, 1, 2, 1)
    if actual != expected:
        raise AssertionError(f"{stage} expected {expected}, got {actual}")
    print(f"{stage}: PASS — 2 unique lines, corrected quantity=3, revenue=12.5000, 2 batches; Gold=1/2/1 rows")


def main() -> int:
    load_dotenv(ROOT / ".env")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    database = f"retail_phase9_verify_{stamp}"
    run_root = ROOT / "data" / "bench" / f"phase9_backfill_{stamp}"
    data_dir = run_root / "data"
    input_dir = run_root / "input"
    log_dir = ROOT / "logs" / "bench" / f"phase9_backfill_{stamp}"
    first, second = write_fixtures(input_dir)

    print(f"Creating isolated verification database: {database}")
    print("The configured retail database will not be used or modified.")
    create_scratch_database(database)
    initialize_schema(database)

    env = os.environ.copy()
    env.update({
        "DATA_DIR": str(data_dir),
        "LOG_DIR": str(log_dir),
        "WAREHOUSE_ENABLED": "true",
        "PGDATABASE": database,
    })

    print("\nLoading initial batch")
    run_pipeline(env, "--input", str(first))
    print("\nLoading correction/new line in second batch")
    run_pipeline(env, "--input", str(second))
    assert_expected(snapshot(database), "After incremental correction")

    from retail_pipeline.audit import fingerprint

    first_batch_id = fingerprint(first)
    for replay_number in (1, 2):
        print(f"\nReplaying historical batch 1 (backfill check {replay_number}/2)")
        run_pipeline(env, "--backfill-batch", first_batch_id)
        assert_expected(snapshot(database), f"After backfill replay {replay_number}")

    print(f"\nSUCCESS: isolated PostgreSQL backfill verification passed.")
    print(f"Scratch database retained for review: {database}")
    print(f"Fixture and pipeline data retained under: {run_root}")
    print("Review the database/data before dropping them; no cleanup is automatic.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
