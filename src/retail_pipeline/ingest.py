import csv
import logging
from datetime import date, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

EXPECTED = ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
OPTIONAL_LINE_IDS = ["LineID", "line_id", "LineItemID"]
SOURCE_SCHEMA_VERSION = 1


def _xlsx_to_csv(path: Path) -> Path:
    """Stream the workbook into a sibling CSV instead of collecting rows in memory."""
    from openpyxl import load_workbook

    csv_path = path.with_suffix(".csv")
    if csv_path.exists() and csv_path.stat().st_mtime >= path.stat().st_mtime:
        logging.info("Using existing streamed CSV: %s", csv_path)
        return csv_path
    logging.info("Streaming Excel workbook to CSV; this may take a minute: %s", path)
    workbook = load_workbook(path, read_only=True, data_only=True)
    try:
        rows = workbook.active.iter_rows(values_only=True)
        try:
            headers = [str(value).strip() if value is not None else "" for value in next(rows)]
        except StopIteration as exc:
            raise ValueError("The workbook is empty") from exc
        missing = set(EXPECTED) - set(headers)
        if missing:
            raise ValueError(f"Source is missing required columns: {sorted(missing)}")
        # Keep every input column in Bronze. read_source separates the stable
        # contract from additive fields and stores unknown values as JSON.
        positions = list(range(len(headers)))
        output_headers = headers
        with csv_path.open("w", encoding="utf-8", newline="") as output:
            writer = csv.writer(output)
            writer.writerow(output_headers)
            for row in rows:
                values = []
                for position in positions:
                    value = row[position] if position < len(row) else None
                    if isinstance(value, (datetime, date)):
                        value = value.isoformat(sep=" ") if isinstance(value, datetime) else value.isoformat()
                    values.append(value)
                writer.writerow(values)
    finally:
        workbook.close()
    logging.info("Finished streaming workbook to CSV: %s", csv_path)
    return csv_path


def read_source(spark: SparkSession, path: Path) -> DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xlsm"}:
        path = _xlsx_to_csv(path)
    elif suffix != ".csv":
        raise ValueError("Input must be a .xlsx, .xlsm, or .csv file")
    source = spark.read.option("header", True).option("inferSchema", False).csv(str(path))
    # Normalize optional source-provided immutable line identifiers and retain
    # additive columns long enough to serialize their values into the raw contract.
    line_id_col = next((name for name in OPTIONAL_LINE_IDS + ["_input_line_id"] if name in source.columns), None)
    missing = set(EXPECTED) - set(source.columns)
    if missing:
        raise ValueError(f"Source is missing required columns: {sorted(missing)}")
    known = set(EXPECTED + OPTIONAL_LINE_IDS + ["_input_line_id"])
    extra_columns = [name for name in source.columns if name not in known]
    selected_columns = EXPECTED + extra_columns
    if line_id_col and line_id_col not in selected_columns:
        selected_columns.append(line_id_col)
    selected = source.select(*selected_columns)
    if line_id_col:
        selected = selected.withColumn("_input_line_id", F.trim(F.col(line_id_col).cast("string")))
        if line_id_col != "_input_line_id":
            selected = selected.drop(line_id_col)
    else:
        selected = selected.withColumn("_input_line_id", F.lit(None).cast("string"))
    if extra_columns:
        extras = F.to_json(F.struct(*[selected[name].alias(name) for name in extra_columns]))
    else:
        extras = F.lit("{}")
    # Serialize extras before dropping their physical source columns. Spark
    # resolves expressions lazily, so dropping first leaves ``extras`` with
    # references to columns that no longer exist in the input projection.
    return (selected
        .withColumn("_source_extras", extras)
        .drop(*extra_columns)
        .withColumn("_source_extra_columns", F.lit(",".join(extra_columns)))
        .withColumn("_source_schema_version", F.lit(SOURCE_SCHEMA_VERSION)))


def with_ingest_metadata(frame: DataFrame, source_name: str, fingerprint: str, run_id: str | None = None) -> DataFrame:
    return (frame.withColumn("_source_file", F.lit(source_name))
            .withColumn("_source_fingerprint", F.lit(fingerprint))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_run_id", F.lit(run_id)))
