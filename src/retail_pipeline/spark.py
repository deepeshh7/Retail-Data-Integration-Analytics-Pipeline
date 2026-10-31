import os
import sys

from pyspark.sql import SparkSession


def create_spark(master: str = "local[*]") -> SparkSession:
    # On Windows Spark starts separate Python workers. Point them at the active
    # venv interpreter instead of the Microsoft Store `python` command alias.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    # Benchmarked on the local 541k-row retail source: 4 partitions reduced
    # wall time and output files versus 8 on the development WSL machine.
    shuffle_partitions = max(1, int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "4")))
    max_records_per_file = max(0, int(os.getenv("SPARK_MAX_RECORDS_PER_FILE", "0")))
    return (
        SparkSession.builder.appName("retail-data-pipeline")
        .master(master)
        .config("spark.pyspark.python", sys.executable)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.files.maxRecordsPerFile", str(max_records_per_file))
        .getOrCreate()
    )
