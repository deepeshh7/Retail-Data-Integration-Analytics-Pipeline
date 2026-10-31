from datetime import datetime
from decimal import Decimal
import os
import sys

import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (DecimalType, LongType, StringType, StructField,
                               StructType, TimestampType)

from retail_pipeline.runner import _all_silver_batches, _batch_ingestion_time


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    session = (SparkSession.builder.master("local[2]").appName("retail-backfill-tests")
               .config("spark.pyspark.python", sys.executable)
               .config("spark.ui.enabled", "false").getOrCreate())
    yield session
    session.stop()


def test_replaying_older_batch_does_not_override_newer_line_version(spark, tmp_path):
    schema = StructType([
        StructField("invoice_no", StringType(), False),
        StructField("stock_code", StringType(), False),
        StructField("description", StringType(), False),
        StructField("quantity", LongType(), False),
        StructField("invoice_ts", TimestampType(), False),
        StructField("unit_price", DecimalType(12, 4), False),
        StructField("customer_id", LongType(), True),
        StructField("country", StringType(), False),
        StructField("_input_line_id", StringType(), False),
        StructField("_source_fingerprint", StringType(), False),
        StructField("_ingested_at", TimestampType(), False),
    ])

    def batch(quantity, fingerprint, ingested_at):
        return spark.createDataFrame([(
            "900001", "A", "Mug", quantity, datetime(2011, 1, 1, 10),
            Decimal("3.5000"), 7, "United Kingdom", "stable-line-1",
            fingerprint, ingested_at,
        )], schema)

    old_fingerprint = "a" * 64
    newer_fingerprint = "b" * 64
    old_ingested_at = datetime(2026, 1, 1, 12)
    newer_ingested_at = datetime(2026, 1, 2, 12)
    old_path = tmp_path / "silver_batches" / f"source_fingerprint={old_fingerprint}"
    newer_path = tmp_path / "silver_batches" / f"source_fingerprint={newer_fingerprint}"
    batch(2, old_fingerprint, old_ingested_at).write.parquet(str(old_path))
    batch(3, newer_fingerprint, newer_ingested_at).write.parquet(str(newer_path))

    # Reprocessing recalculates the old batch now, but its original ordering
    # timestamp must survive so it cannot supersede the later correction.
    original = _batch_ingestion_time(spark, old_path)
    assert original is not None
    replay = batch(2, old_fingerprint, datetime(2026, 10, 31, 12))
    replay = replay.withColumn("_ingested_at", F.lit(original).cast("timestamp"))
    replay.write.mode("overwrite").parquet(str(old_path))

    current = _all_silver_batches(spark, tmp_path)
    selected = current.select("quantity", "_source_fingerprint").collect()
    assert len(selected) == 1
    assert selected[0]["quantity"] == 3
    assert selected[0]["_source_fingerprint"] == newer_fingerprint
