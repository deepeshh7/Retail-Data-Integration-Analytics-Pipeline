"""Stage functions used by the Airflow DAG.

The functions deliberately exchange only small metadata dictionaries. The data
itself stays in the same fingerprinted Parquet layers used by manual runs.
"""

import hashlib
import logging
import shutil
import time
from decimal import Decimal
from pathlib import Path

from .audit import fingerprint, is_completed, mark_completed, write_run_record
from .config import Settings
from .ingest import read_source, with_ingest_metadata
from .quality import standardize, validate
from .spark import create_spark
from .transform import gold, silver
from .warehouse import apply_incremental_batch, read_gold_tables
from .runner import (
    _has_parquet_files,
    _merge_legacy_silver,
    _persist_gold,
    _quality_reason_counts,
    _verify_gold,
)


def airflow_run_id(dag_id: str, airflow_run_id: str) -> str:
    """Return a stable, path-safe run ID across task retries."""
    value = f"{dag_id}:{airflow_run_id}".encode("utf-8")
    return hashlib.sha256(value).hexdigest()[:32]


def _audit(settings: Settings, record: dict) -> None:
    write_run_record(settings.data_dir / "audit", record)


def _audit_failure(settings: Settings, metadata: dict, stage: str, started: float, exc: Exception) -> None:
    record = {
        "run_id": metadata.get("run_id"),
        "airflow_run_id": metadata.get("airflow_run_id"),
        "batch_id": metadata.get("batch_id"),
        "source_file": metadata.get("source_file"),
        "status": "failed",
        "stage": stage,
        "duration_seconds": round(time.monotonic() - started, 3),
        "error": {"type": type(exc).__name__, "message": str(exc)},
    }
    try:
        _audit(settings, record)
    except Exception:
        logging.exception("Could not write Airflow stage failure audit")


def _stop_spark(spark) -> None:
    """Clean up Spark without hiding a stage's original error or result."""
    if spark is not None:
        try:
            spark.stop()
        except Exception:
            logging.exception("Spark cleanup failed after Airflow stage")


def ingest_batch(source_path: str | None, airflow_run: dict, force: bool = False) -> dict:
    """Read source and write the fingerprinted Bronze layer."""
    settings = Settings.from_env()
    source = Path(source_path) if source_path else settings.input_path
    source = source.resolve()
    metadata = {
        "run_id": airflow_run_id(airflow_run["dag_id"], airflow_run["run_id"]),
        "airflow_run_id": airflow_run["run_id"],
        "source_path": str(source),
        "source_file": source.name,
    }
    started = time.monotonic()
    stage = "airflow_ingestion"
    spark = None
    try:
        if not source.is_file():
            raise FileNotFoundError(f"Input file not found: {source}")
        batch_id = fingerprint(source)
        metadata["batch_id"] = batch_id
        manifest = settings.data_dir / "audit" / "manifest.json"
        if not force and is_completed(manifest, batch_id):
            metadata["skip"] = True
            metadata["skip_reason"] = "source fingerprint is already complete"
            logging.info("Airflow ingestion skipped for completed batch %s", batch_id)
            return metadata

        spark = create_spark(settings.spark_master)
        raw_copy = settings.data_dir / "raw" / batch_id / source.name
        raw_copy.parent.mkdir(parents=True, exist_ok=True)
        if not raw_copy.exists():
            shutil.copy2(source, raw_copy)

        raw = with_ingest_metadata(read_source(spark, raw_copy), source.name, batch_id, metadata["run_id"]).cache()
        source_rows = int(raw.count())
        bronze_path = settings.data_dir / "bronze" / f"source_fingerprint={batch_id}"
        raw.write.mode("overwrite").parquet(str(bronze_path))
        bronze_rows = int(spark.read.parquet(str(bronze_path)).count())
        if bronze_rows != source_rows:
            raise ValueError(f"Bronze row count mismatch: source={source_rows}, bronze={bronze_rows}")
        metadata["source_rows"] = source_rows
        metadata["bronze_rows"] = bronze_rows
        logging.info("Airflow ingestion wrote %s Bronze rows for batch %s", bronze_rows, batch_id)
        return metadata
    except Exception as exc:
        _audit_failure(settings, metadata, stage, started, exc)
        raise
    finally:
        _stop_spark(spark)


def transform_batch(metadata: dict) -> dict:
    """Validate Bronze rows, write quarantine, and prepare batch Silver Parquet."""
    if metadata.get("skip"):
        return metadata
    settings = Settings.from_env()
    batch_id = metadata["batch_id"]
    started = time.monotonic()
    stage = "airflow_transformation"
    spark = None
    try:
        spark = create_spark(settings.spark_master)
        bronze_path = settings.data_dir / "bronze" / f"source_fingerprint={batch_id}"
        if not _has_parquet_files(bronze_path):
            raise FileNotFoundError(f"Bronze batch not found for fingerprint {batch_id}: {bronze_path}")
        bronze = spark.read.parquet(str(bronze_path))
        accepted, rejected = validate(standardize(bronze))
        accepted_rows = int(accepted.count())
        rejected_rows = int(rejected.count())
        if metadata["source_rows"] != accepted_rows + rejected_rows:
            raise ValueError(
                f"Row reconciliation failed: source={metadata['source_rows']}, "
                f"accepted={accepted_rows}, quarantined={rejected_rows}"
            )

        quarantine_path = settings.data_dir / "quarantine" / f"source_fingerprint={batch_id}"
        rejected.write.mode("overwrite").parquet(str(quarantine_path))
        quarantine_parquet_rows = int(spark.read.parquet(str(quarantine_path)).count())
        if quarantine_parquet_rows != rejected_rows:
            raise ValueError(
                f"Quarantine row count mismatch: rejected={rejected_rows}, "
                f"parquet={quarantine_parquet_rows}"
            )

        clean = silver(accepted).cache()
        silver_rows = int(clean.count())
        if silver_rows > accepted_rows:
            raise ValueError(f"Silver row count exceeds accepted rows: silver={silver_rows}, accepted={accepted_rows}")
        silver_path = settings.data_dir / "silver_batches" / f"source_fingerprint={batch_id}"
        clean.write.mode("overwrite").parquet(str(silver_path))
        silver_parquet_rows = int(spark.read.parquet(str(silver_path)).count())
        if silver_parquet_rows != silver_rows:
            raise ValueError(f"Silver batch row count mismatch: dataframe={silver_rows}, parquet={silver_parquet_rows}")

        metadata.update({
            "accepted_rows": accepted_rows,
            "quarantined_rows": rejected_rows,
            "deduplicated_rows": accepted_rows - silver_rows,
            "silver_rows": silver_rows,
            "rejection_reasons": _quality_reason_counts(rejected),
        })
        logging.info(
            "Airflow transformation prepared %s Silver rows and quarantined %s rows for batch %s",
            silver_rows, rejected_rows, batch_id,
        )
        return metadata
    except Exception as exc:
        _audit_failure(settings, metadata, stage, started, exc)
        raise
    finally:
        _stop_spark(spark)


def load_batch(metadata: dict) -> dict:
    """Idempotently load Silver into the warehouse and reconcile Gold outputs."""
    if metadata.get("skip"):
        return metadata
    settings = Settings.from_env()
    batch_id = metadata["batch_id"]
    started = time.monotonic()
    stage = "airflow_warehouse_load"
    spark = None
    try:
        spark = create_spark(settings.spark_master)
        silver_path = settings.data_dir / "silver_batches" / f"source_fingerprint={batch_id}"
        if not _has_parquet_files(silver_path):
            raise FileNotFoundError(f"Silver batch not found for fingerprint {batch_id}: {silver_path}")
        clean = spark.read.parquet(str(silver_path))
        if settings.warehouse_enabled:
            legacy_silver = settings.data_dir / "silver"
            baseline = spark.read.parquet(str(legacy_silver)) if _has_parquet_files(legacy_silver) else None
            applied = apply_incremental_batch(
                settings.pg_connect_args(), batch_id, metadata["source_file"], clean,
                baseline, metadata["silver_rows"],
            )
            gold_tables = read_gold_tables(spark, settings.pg_connect_args())
            gold_metrics = _persist_gold(spark, gold_tables, settings.data_dir / "gold", metadata["run_id"])
            persisted_gold = {
                name: spark.read.parquet(str(settings.data_dir / "gold" / name))
                for name in gold_tables
            }
            gold_metrics = _verify_gold(persisted_gold)
            metadata["warehouse_batch_applied"] = applied
        else:
            silver_all = _merge_legacy_silver(spark, clean, settings.data_dir)
            from pyspark.sql import functions as F
            silver_total = silver_all.agg(F.sum("line_amount").alias("net")).first()["net"] or Decimal("0.0000")
            gold_tables = gold(silver_all)
            _persist_gold(spark, gold_tables, settings.data_dir / "gold", metadata["run_id"])
            persisted_gold = {
                name: spark.read.parquet(str(settings.data_dir / "gold" / name))
                for name in gold_tables
            }
            gold_metrics = _verify_gold(persisted_gold, expected_silver_total=silver_total)
            metadata["silver_current_net_revenue"] = str(silver_total)

        metadata["gold_rows"] = {name: metric["rows"] for name, metric in gold_metrics.items()}
        metadata["gold_net_revenue"] = {name: str(metric["net_revenue"]) for name, metric in gold_metrics.items()}
        mark_completed(settings.data_dir / "audit" / "manifest.json", batch_id)
        metadata.update({
            "status": "completed",
            "stage": "airflow_warehouse_load",
            "duration_seconds": round(time.monotonic() - started, 3),
        })
        _audit(settings, metadata)
        logging.info("Airflow pipeline completed batch %s", batch_id)
        return metadata
    except Exception as exc:
        _audit_failure(settings, metadata, stage, started, exc)
        raise
    finally:
        _stop_spark(spark)
