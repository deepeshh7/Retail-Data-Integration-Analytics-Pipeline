import logging
import shutil
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from .audit import fingerprint, is_completed, mark_completed, write_run_record
from .config import Settings
from .ingest import read_source, with_ingest_metadata
from .quality import standardize, validate
from .spark import create_spark
from .transform import gold, silver
from .warehouse import (apply_incremental_batch, is_batch_processed,
                        read_gold_tables, rebuild_warehouse_state)


class PipelineFailure(RuntimeError):
    def __init__(self, run_id: str, stage: str, cause: Exception):
        self.run_id = run_id
        self.stage = stage
        self.cause = cause
        super().__init__(f"{type(cause).__name__}: {cause}")


def _has_parquet_files(directory: Path) -> bool:
    return directory.is_dir() and any(directory.glob("*.parquet"))


def _find_batch_source(data_dir: Path, batch_id: str) -> Path:
    if len(batch_id) != 64 or any(ch not in "0123456789abcdef" for ch in batch_id.lower()):
        raise ValueError("Backfill batch ID must be a 64-character SHA-256 fingerprint")
    batch_dir = data_dir / "raw" / batch_id
    candidates = [path for suffix in (".xlsx", ".xlsm", ".csv")
                  for path in batch_dir.glob(f"*{suffix}")]
    if not candidates:
        raise FileNotFoundError(f"No retained source file for batch {batch_id} under {batch_dir}")
    return candidates[0]


def _all_silver_batches(spark, data_dir: Path, include_legacy: bool = True):
    from pyspark.sql import Window, functions as F
    from .transform import with_line_id

    paths = sorted(path for path in (data_dir / "silver_batches").glob("source_fingerprint=*")
                   if _has_parquet_files(path))
    frames = [spark.read.option("mergeSchema", "true").parquet(str(path)) for path in paths]
    legacy = data_dir / "silver"
    if include_legacy and _has_parquet_files(legacy):
        frames.append(spark.read.option("mergeSchema", "true").parquet(str(legacy)))
    if not frames:
        raise FileNotFoundError(f"No retained Silver batches found under {data_dir / 'silver_batches'}")
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame, allowMissingColumns=True)
    identified = with_line_id(combined)
    latest = Window.partitionBy("line_id").orderBy(
        F.col("_ingested_at").desc_nulls_last(),
        F.col("_source_fingerprint").desc_nulls_last(),
    )
    return identified.withColumn("_latest", F.row_number().over(latest)).filter("_latest=1").drop("_latest")


def _batch_ingestion_time(spark, batch_path: Path):
    """Return the original ingest timestamp so a replay cannot outrank newer batches."""
    if not _has_parquet_files(batch_path):
        return None
    from pyspark.sql import functions as F

    previous = spark.read.parquet(str(batch_path))
    if "_ingested_at" not in previous.columns:
        return None
    return previous.agg(F.min("_ingested_at").alias("original_ingested_at")).first()[0]


def _move_directory_contents(source: Path, destination: Path) -> None:
    """Move Spark output into an existing empty/placeholder layer directory."""
    destination.mkdir(parents=True, exist_ok=True)
    for child in source.iterdir():
        shutil.move(str(child), str(destination / child.name))
    source.rmdir()


def _merge_legacy_silver(spark, clean, data_dir: Path):
    """Full rebuild fallback, including batches previously written incrementally."""
    silver_path = data_dir / "silver"
    new_path = data_dir / "silver_incoming"
    clean.write.mode("overwrite").parquet(str(new_path))
    inputs = []
    if _has_parquet_files(silver_path):
        inputs.append(spark.read.parquet(str(silver_path)))
    batch_root = data_dir / "silver_batches"
    if batch_root.is_dir() and any(batch_root.glob("source_fingerprint=*")):
        inputs.append(spark.read.option("mergeSchema", "true").parquet(str(batch_root)))
    inputs.append(spark.read.parquet(str(new_path)))
    if len(inputs) == 1 and not _has_parquet_files(silver_path):
        _move_directory_contents(new_path, silver_path)
    else:
        from pyspark.sql import Window, functions as F
        from .transform import with_line_id
        combined = inputs[0]
        for extra in inputs[1:]:
            combined = combined.unionByName(extra, allowMissingColumns=True)
        combined = with_line_id(combined)
        latest = Window.partitionBy("line_id").orderBy(F.col("_ingested_at").desc_nulls_last())
        combined = combined.withColumn("_latest", F.row_number().over(latest)).filter("_latest=1").drop("_latest")
        staging = data_dir / "silver_staging"
        combined.write.mode("overwrite").parquet(str(staging))
        if silver_path.exists():
            shutil.rmtree(silver_path)
        staging.rename(silver_path)
        shutil.rmtree(new_path)
    return spark.read.parquet(str(silver_path))


def _frame_summary(frame, sum_column: str) -> dict:
    from pyspark.sql import functions as F
    row = frame.agg(F.count(F.lit(1)).alias("rows"), F.sum(F.col(sum_column)).alias("total")).first()
    return {"rows": int(row["rows"]), "net_revenue": row["total"] or Decimal("0.0000")}


def _verify_gold(gold_tables: dict, expected_silver_total: Decimal | None = None) -> dict:
    summaries = {name: _frame_summary(frame, "net_revenue") for name, frame in gold_tables.items()}
    totals = [summary["net_revenue"] for summary in summaries.values()]
    if totals and any(total != totals[0] for total in totals[1:]):
        raise ValueError(f"Gold revenue reconciliation failed: {summaries}")
    if expected_silver_total is not None and totals and any(total != expected_silver_total for total in totals):
        raise ValueError(f"Silver-to-Gold net revenue reconciliation failed: silver={expected_silver_total}, gold={summaries}")
    return summaries


def _persist_gold(spark, gold_tables: dict, gold_dir: Path, run_id: str) -> dict:
    """Write and re-read Gold outputs so reconciliation covers persisted Parquet."""
    summaries = {}
    for name, frame in gold_tables.items():
        path = gold_dir / name
        logging.info("Writing Gold table", extra={"event": "layer_write_started", "run_id": run_id, "stage": "gold", "table": name})
        frame.write.mode("overwrite").parquet(str(path))
        summaries[name] = _frame_summary(spark.read.parquet(str(path)), "net_revenue")
    return summaries

def _quality_reason_counts(rejected) -> dict[str, int]:
    return {row["_rejection_reasons"]: int(row["count"])
            for row in rejected.groupBy("_rejection_reasons").count().collect()}


def _audit(settings: Settings, record: dict, run_id: str) -> Path:
    path = write_run_record(settings.data_dir / "audit", record)
    logging.info("Run audit written", extra={"event": "audit_written", "run_id": run_id, "audit_path": str(path), "stage": "audit"})
    return path


def run(settings: Settings, input_path: Path | None = None, force: bool = False,
        dry_run: bool = False, run_id: str | None = None,
        backfill_batch_id: str | None = None) -> dict:
    run_id = run_id or uuid.uuid4().hex
    started = datetime.now(timezone.utc)
    clock_start = time.monotonic()
    stage = "preflight"
    source = (input_path or settings.input_path).resolve()
    digest = None
    counts = {}
    quality_reasons = {}
    gold_metrics = {}
    spark = None
    try:
        if backfill_batch_id:
            source = _find_batch_source(settings.data_dir, backfill_batch_id).resolve()
        if not source.is_file():
            raise FileNotFoundError(f"Input file not found: {source}. Download UCI Online Retail and pass --input.")
        digest = fingerprint(source)
        manifest = settings.data_dir / "audit" / "manifest.json"
        logging.info("Pipeline run started", extra={"event": "run_started", "run_id": run_id, "batch_id": digest, "stage": stage, "source_file": source.name})

        if backfill_batch_id and digest != backfill_batch_id:
            raise ValueError(f"Retained source fingerprint {digest} does not match requested batch {backfill_batch_id}")
        if not force and not backfill_batch_id and is_completed(manifest, digest):
            record = {"run_id": run_id, "batch_id": digest, "source_file": source.name,
                      "status": "skipped", "stage": stage, "started_at_utc": started.isoformat(),
                      "duration_seconds": round(time.monotonic() - clock_start, 3)}
            _audit(settings, record, run_id)
            logging.info("Skipping completed batch", extra={"event": "run_skipped", "run_id": run_id, "batch_id": digest, "stage": stage})
            return record
        if (settings.warehouse_enabled and not dry_run and not backfill_batch_id
                and is_batch_processed(settings.pg_connect_args(), digest)):
            logging.info("Skipping already applied warehouse batch before Spark", extra={"event": "batch_skipped", "run_id": run_id, "batch_id": digest, "stage": stage})
            if not is_completed(manifest, digest):
                stage = "gold_recovery"
                spark = create_spark(settings.spark_master)
                for name, frame in read_gold_tables(spark, settings.pg_connect_args()).items():
                    frame.write.mode("overwrite").parquet(str(settings.data_dir / "gold" / name))
                mark_completed(manifest, digest)
            record = {"run_id": run_id, "batch_id": digest, "source_file": source.name,
                      "status": "skipped", "stage": stage, "started_at_utc": started.isoformat(),
                      "duration_seconds": round(time.monotonic() - clock_start, 3)}
            _audit(settings, record, run_id)
            return record

        stage = "spark_startup"
        spark = create_spark(settings.spark_master)
        raw_copy = settings.data_dir / "raw" / digest / source.name
        raw_copy.parent.mkdir(parents=True, exist_ok=True)
        if not raw_copy.exists():
            shutil.copy2(source, raw_copy)

        stage = "ingestion"
        logging.info("Reading source", extra={"event": "stage_started", "run_id": run_id, "batch_id": digest, "stage": stage})
        raw = with_ingest_metadata(read_source(spark, raw_copy), source.name, digest, run_id).cache()
        counts["source"] = int(raw.count())
        counts["source_schema_version"] = raw.select("_source_schema_version").first()[0]
        counts["source_extra_columns"] = raw.select("_source_extra_columns").first()[0]

        stage = "bronze"
        bronze_path = settings.data_dir / "bronze" / f"source_fingerprint={digest}"
        raw.write.mode("overwrite").parquet(str(bronze_path))
        counts["bronze"] = int(spark.read.parquet(str(bronze_path)).count())
        if counts["bronze"] != counts["source"]:
            raise ValueError(f"Bronze row count mismatch: source={counts['source']}, bronze={counts['bronze']}")
        logging.info("Bronze written", extra={"event": "layer_written", "run_id": run_id, "batch_id": digest, "stage": stage, "row_count": counts["bronze"]})

        stage = "quality"
        accepted, rejected = validate(standardize(raw))
        counts["accepted"] = int(accepted.count())
        counts["quarantined"] = int(rejected.count())
        if counts["source"] != counts["accepted"] + counts["quarantined"]:
            raise ValueError(f"Row reconciliation failed: source={counts['source']}, accepted={counts['accepted']}, quarantined={counts['quarantined']}")
        quarantine_path = settings.data_dir / "quarantine" / f"source_fingerprint={digest}"
        rejected.write.mode("overwrite").parquet(str(quarantine_path))
        counts["quarantine_parquet"] = int(spark.read.parquet(str(quarantine_path)).count())
        if counts["quarantine_parquet"] != counts["quarantined"]:
            raise ValueError(f"Quarantine row count mismatch: rejected={counts['quarantined']}, parquet={counts['quarantine_parquet']}")
        quality_reasons = _quality_reason_counts(rejected)
        logging.info("Quality checks complete", extra={"event": "quality_complete", "run_id": run_id, "batch_id": digest, "stage": stage, "row_count": counts["source"], "accepted": counts["accepted"], "quarantined": counts["quarantined"], "rejection_reasons": quality_reasons})

        stage = "silver"
        clean = silver(accepted)
        if backfill_batch_id:
            existing_batch = settings.data_dir / "silver_batches" / f"source_fingerprint={digest}"
            original_ingested_at = _batch_ingestion_time(spark, existing_batch)
            if original_ingested_at is not None:
                # A replay gets a new run ID for lineage, but retains the batch's
                # original ordering timestamp. Otherwise an old batch replay
                # could replace a newer version of the same stable line ID.
                from pyspark.sql import functions as F
                clean = clean.withColumn("_ingested_at", F.lit(original_ingested_at).cast("timestamp"))
        clean = clean.cache()
        counts["silver"] = int(clean.count())
        counts["deduplicated"] = counts["accepted"] - counts["silver"]
        if counts["silver"] > counts["accepted"]:
            raise ValueError(f"Silver row count exceeds accepted rows: silver={counts['silver']}, accepted={counts['accepted']}")
        logging.info("Silver batch prepared", extra={"event": "layer_prepared", "run_id": run_id, "batch_id": digest, "stage": stage, "row_count": counts["silver"], "deduplicated": counts["deduplicated"]})

        stage = "warehouse" if settings.warehouse_enabled and not dry_run else "gold"
        if settings.warehouse_enabled and not dry_run:
            silver_path = settings.data_dir / "silver"
            baseline = spark.read.parquet(str(silver_path)) if _has_parquet_files(silver_path) else None
            batch_silver = settings.data_dir / "silver_batches" / f"source_fingerprint={digest}"
            batch_silver.parent.mkdir(parents=True, exist_ok=True)
            clean.write.mode("overwrite").parquet(str(batch_silver))
            counts["silver_parquet"] = int(spark.read.parquet(str(batch_silver)).count())
            if counts["silver_parquet"] != counts["silver"]:
                raise ValueError(f"Silver batch row count mismatch: dataframe={counts['silver']}, parquet={counts['silver_parquet']}")
            if backfill_batch_id:
                current_silver = _all_silver_batches(spark, settings.data_dir)
                current_count = int(current_silver.count())
                rebuild_warehouse_state(settings.pg_connect_args(), digest, source.name, current_silver, current_count)
                applied = True
                counts["backfill_rebuilt_silver_rows"] = current_count
            else:
                applied = apply_incremental_batch(settings.pg_connect_args(), digest, source.name, clean, baseline, counts["silver"])
            logging.info("Warehouse batch processed", extra={"event": "warehouse_batch", "run_id": run_id, "batch_id": digest, "stage": stage, "applied": applied})
            gold_tables = read_gold_tables(spark, settings.pg_connect_args())
            stage = "gold"
            gold_metrics = _persist_gold(spark, gold_tables, settings.data_dir / "gold", run_id)
            persisted_gold = {name: spark.read.parquet(str(settings.data_dir / "gold" / name)) for name in gold_tables}
            gold_metrics = _verify_gold(persisted_gold)
        else:
            silver_all = _merge_legacy_silver(spark, clean, settings.data_dir)
            counts["silver_current"] = int(silver_all.count())
            counts["silver_parquet"] = counts["silver_current"]
            from pyspark.sql import functions as F
            silver_total = silver_all.agg(F.sum("line_amount").alias("net")).first()["net"] or Decimal("0.0000")
            gold_tables = gold(silver_all)
            gold_metrics = _persist_gold(spark, gold_tables, settings.data_dir / "gold", run_id)
            persisted_gold = {name: spark.read.parquet(str(settings.data_dir / "gold" / name)) for name in gold_tables}
            stage = "reconciliation"
            gold_metrics = _verify_gold(persisted_gold, expected_silver_total=silver_total)
            counts["silver_net_revenue"] = str(silver_total)

        stage = "reconciliation"
        if settings.warehouse_enabled and not dry_run:
            gold_metrics = _verify_gold(gold_tables)
        counts["gold_rows"] = {name: metrics["rows"] for name, metrics in gold_metrics.items()}
        counts["gold_net_revenue"] = {name: str(metrics["net_revenue"]) for name, metrics in gold_metrics.items()}
        counts["rejection_reasons"] = quality_reasons

        stage = "manifest"
        if not dry_run:
            mark_completed(manifest, digest)
        record = {"run_id": run_id, "batch_id": digest, "source_file": source.name,
                  "status": "completed", "stage": stage, "started_at_utc": started.isoformat(),
                  "duration_seconds": round(time.monotonic() - clock_start, 3), **counts}
        _audit(settings, record, run_id)
        logging.info("Pipeline completed", extra={"event": "run_completed", "run_id": run_id, "batch_id": digest, "stage": stage, "duration_seconds": record["duration_seconds"], "metrics": counts})
        return record
    except Exception as exc:
        record = {"run_id": run_id, "batch_id": digest, "source_file": source.name,
                  "status": "failed", "stage": stage, "started_at_utc": started.isoformat(),
                  "duration_seconds": round(time.monotonic() - clock_start, 3),
                  "counts": counts, "rejection_reasons": quality_reasons,
                  "error": {"type": type(exc).__name__, "message": str(exc)}}
        try:
            _audit(settings, record, run_id)
        except Exception as audit_exc:
            logging.error("Could not write failure audit", extra={"event": "audit_write_failed", "run_id": run_id, "stage": "audit", "error_type": type(audit_exc).__name__, "error_message": str(audit_exc)})
        logging.error("Pipeline failed", extra={"event": "run_failed", "run_id": run_id, "batch_id": digest, "stage": stage, "error_type": type(exc).__name__, "error_message": str(exc)})
        raise PipelineFailure(run_id, stage, exc) from exc
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as stop_exc:
                logging.error("Spark cleanup failed", extra={"event": "cleanup_failed", "run_id": run_id, "stage": "cleanup", "error_type": type(stop_exc).__name__, "error_message": str(stop_exc)})


def write_audit(settings: Settings, record: dict) -> Path:
    return write_run_record(settings.data_dir / "audit", record)
