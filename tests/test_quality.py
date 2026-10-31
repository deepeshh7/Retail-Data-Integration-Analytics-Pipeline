import os
import sys
import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from retail_pipeline.quality import standardize, validate
from retail_pipeline.transform import gold, silver


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    session = (SparkSession.builder.master("local[2]").appName("retail-tests")
               .config("spark.pyspark.python", sys.executable)
               .config("spark.ui.enabled", "false").getOrCreate())
    yield session
    session.stop()


def test_quality_quarantines_invalid_and_keeps_returns(spark):
    rows = [
        ("100", "A", "Mug", 2, "2011-01-01 10:00:00", 3.5, 7, "UK"),
        ("C101", "A", "Mug", -1, "2011-01-02 10:00:00", 3.5, 7, "UK"),
        ("102", "B", "Bad", 0, "2011-01-02 10:00:00", 2.0, 8, "UK"),
    ]
    frame = spark.createDataFrame(rows, ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"])
    from retail_pipeline.ingest import with_ingest_metadata
    frame = with_ingest_metadata(frame, "test.csv", "abc123")
    accepted, rejected = validate(standardize(frame))
    assert accepted.count() == 2
    assert rejected.select("_rejection_reasons").first()[0] == "invalid_quantity"
    returns = accepted.filter("is_return").first()
    assert returns.is_cancellation


def test_silver_and_gold_net_returns(spark):
    rows = [
        ("100", "A", "Mug", 2, "2011-01-01 10:00:00", 3.5, 7, "UK"),
        ("C101", "A", "Mug", -1, "2011-01-02 10:00:00", 3.5, 7, "UK"),
    ]
    frame = spark.createDataFrame(rows, ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"])
    from retail_pipeline.ingest import with_ingest_metadata
    frame = with_ingest_metadata(frame, "test.csv", "abc123")
    accepted, _ = validate(standardize(frame))
    clean = silver(accepted)
    assert clean.count() == 2
    tables = gold(clean)
    totals = [sum((row.net_revenue for row in frame.collect()), 0) for frame in tables.values()]
    assert totals == [3.5, 3.5, 3.5]


def test_explicit_line_id_is_stable_when_measures_are_corrected(spark):
    from retail_pipeline.transform import with_line_id

    rows = [
        ("100", "A", "Mug", 2, "2011-01-01 10:00:00", 3.5, 7, "UK", "stable-1"),
        ("100", "A", "Mug", 3, "2011-01-01 10:00:00", 3.5, 7, "UK", "stable-1"),
    ]
    names = ["invoice_no", "stock_code", "description", "quantity", "invoice_ts", "unit_price", "customer_id", "country", "_input_line_id"]
    identified = with_line_id(spark.createDataFrame(rows, names)).select("line_id").distinct()
    assert identified.count() == 1


def test_fallback_event_id_preserves_changed_events(spark):
    from retail_pipeline.transform import with_line_id

    rows = [
        ("100", "A", "Mug", 2, "2011-01-01 10:00:00", 3.5, 7, "UK"),
        ("100", "A", "Mug", 3, "2011-01-01 10:00:00", 3.5, 7, "UK"),
    ]
    names = ["invoice_no", "stock_code", "description", "quantity", "invoice_ts", "unit_price", "customer_id", "country"]
    identified = with_line_id(spark.createDataFrame(rows, names)).select("line_id").distinct()
    assert identified.count() == 2


def test_additive_source_columns_are_preserved_as_unmapped_extras(spark, tmp_path):
    from retail_pipeline.ingest import read_source, with_ingest_metadata

    source_path = tmp_path / "with_discount.csv"
    source_path.write_text(
        "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country,Discount\n"
        "100,A,Mug,2,2011-01-01 10:00:00,3.50,7,UK,0.10\n",
        encoding="utf-8",
    )
    raw = with_ingest_metadata(read_source(spark, source_path), source_path.name, "schema-fixture")
    assert raw.select("_source_extra_columns").first()[0] == "Discount"
    assert raw.select("_source_extras").first()[0] == '{"Discount":"0.10"}'

    accepted, rejected = validate(standardize(raw))
    assert rejected.count() == 0
    clean = silver(accepted)
    assert clean.select("_source_extras").first()[0] == '{"Discount":"0.10"}'


def test_conflicting_explicit_line_ids_are_quarantined(spark):
    from retail_pipeline.ingest import with_ingest_metadata

    rows = [
        ("100", "A", "Mug", 2, "2011-01-01 10:00:00", 3.5, 7, "UK", "stable-1"),
        ("100", "A", "Mug", 3, "2011-01-01 10:00:00", 3.5, 7, "UK", "stable-1"),
    ]
    names = ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country", "_input_line_id"]
    frame = with_ingest_metadata(spark.createDataFrame(rows, names), "test.csv", "abc123")
    accepted, rejected = validate(standardize(frame))
    assert accepted.count() == 0
    assert rejected.select("_rejection_reasons").first()[0] == "duplicate_line_id_conflict"


def test_invalid_records_are_quarantined_with_types_and_reason_codes(spark):
    from pyspark.sql.types import DecimalType, LongType
    from retail_pipeline.ingest import with_ingest_metadata

    rows = [
        ("100", "A", "Mug", "2", "2011-01-01 10:00:00", "3.50", "7", "UK"),
        ("", "A", "Missing invoice", "1", "2011-01-01 10:00:00", "1.00", "7", "UK"),
        ("101", "", "Missing stock", "1", "2011-01-01 10:00:00", "1.00", "7", "UK"),
        ("102", "B", "Bad date", "1", "not-a-date", "1.00", "7", "UK"),
        ("103", "C", "Bad quantity", "many", "2011-01-01 10:00:00", "1.00", "7", "UK"),
        ("104", "D", "Zero quantity", "0", "2011-01-01 10:00:00", "1.00", "7", "UK"),
        ("105", "E", "Bad price", "1", "2011-01-01 10:00:00", "not-money", "7", "UK"),
        ("106", "F", "Negative price", "1", "2011-01-01 10:00:00", "-1.00", "7", "UK"),
        ("107", "G", "Missing country", "1", "2011-01-01 10:00:00", "1.00", "7", "   "),
        ("108", "H", "Bad customer id", "1", "2011-01-01 10:00:00", "1.00", "not-an-id", "UK"),
    ]
    names = ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
    raw = with_ingest_metadata(spark.createDataFrame(rows, names), "quality-fixture.csv", "fixture-batch", "fixture-run")
    accepted, rejected = validate(standardize(raw))

    assert accepted.count() == 1
    assert rejected.count() == 9
    assert isinstance(accepted.schema["quantity"].dataType, LongType)
    assert isinstance(accepted.schema["unit_price"].dataType, DecimalType)
    reasons = {row[0] for row in rejected.select("_rejection_reasons").collect()}
    assert reasons == {
        "missing_invoice_no", "missing_stock_code", "invalid_invoice_date",
        "invalid_quantity", "invalid_unit_price", "missing_country", "invalid_customer_id",
    }

    # Spark's Windows output committer can fail on pytest's AppData temp path.
    # Keep this small fixture under the project's ignored tmp/ directory instead.
    project_tmp = Path(__file__).resolve().parents[1] / "tmp"
    project_tmp.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="phase7-quarantine-", dir=project_tmp) as test_dir:
        quarantine_path = Path(test_dir) / "quarantine"
        rejected.write.mode("errorifexists").parquet(str(quarantine_path))
        quarantined = spark.read.parquet(str(quarantine_path))
        assert quarantined.count() == 9
        assert quarantined.filter("_rejection_reasons != ''").count() == 9
        assert quarantined.select("_run_id").distinct().first()[0] == "fixture-run"
