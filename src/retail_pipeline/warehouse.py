from collections.abc import Iterator

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (DateType, DecimalType, LongType, StringType,
                               StructField, StructType)

UPSERTS = {
    "daily_sales": ("sale_date,country,net_revenue,gross_revenue,return_value,invoice_count,line_count", "sale_date,country"),
    "product_sales": ("stock_code,description,net_quantity,net_revenue,invoice_count", "stock_code,description"),
    "country_sales": ("country,net_revenue,invoice_count,customer_count", "country"),
}
FACT_COLUMNS = "line_id,invoice_no,stock_code,description,quantity,invoice_ts,sale_date,unit_price,customer_id,country,is_return,is_cancellation,line_amount,source_fingerprint".split(",")
FACT_COLUMNS += ["source_file", "run_id", "ingested_at"]
FACT_DDL = """
CREATE TABLE IF NOT EXISTS retail_line_items (
 line_id TEXT PRIMARY KEY, invoice_no TEXT NOT NULL, stock_code TEXT NOT NULL,
 description TEXT NOT NULL, quantity BIGINT NOT NULL, invoice_ts TIMESTAMP NOT NULL,
 sale_date DATE NOT NULL, unit_price NUMERIC(12,4) NOT NULL, customer_id BIGINT,
 country TEXT NOT NULL, is_return BOOLEAN NOT NULL, is_cancellation BOOLEAN NOT NULL,
 line_amount NUMERIC(18,4) NOT NULL, source_fingerprint TEXT NOT NULL,
 source_file TEXT, run_id TEXT, ingested_at TIMESTAMP
);
ALTER TABLE retail_line_items ADD COLUMN IF NOT EXISTS source_file TEXT;
ALTER TABLE retail_line_items ADD COLUMN IF NOT EXISTS run_id TEXT;
ALTER TABLE retail_line_items ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMP;
CREATE TABLE IF NOT EXISTS pipeline_batches (
 batch_id TEXT PRIMARY KEY, source_name TEXT NOT NULL, row_count BIGINT NOT NULL,
 applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_retail_line_daily ON retail_line_items(sale_date,country);
CREATE INDEX IF NOT EXISTS ix_retail_line_product ON retail_line_items(stock_code,description);
CREATE INDEX IF NOT EXISTS ix_retail_line_country ON retail_line_items(country);
"""
STAGE_DDL = """
CREATE TEMP TABLE line_stage (
 line_id TEXT NOT NULL, invoice_no TEXT NOT NULL, stock_code TEXT NOT NULL,
 description TEXT NOT NULL, quantity BIGINT NOT NULL, invoice_ts TIMESTAMP NOT NULL,
 sale_date DATE NOT NULL, unit_price NUMERIC(12,4) NOT NULL, customer_id BIGINT,
 country TEXT NOT NULL, is_return BOOLEAN NOT NULL, is_cancellation BOOLEAN NOT NULL,
 line_amount NUMERIC(18,4) NOT NULL, source_fingerprint TEXT NOT NULL,
 source_file TEXT, run_id TEXT, ingested_at TIMESTAMP
) ON COMMIT DROP
"""
GOLD_SCHEMAS = {
 "daily_sales": StructType([StructField("sale_date", DateType(), False), StructField("country", StringType(), False), StructField("net_revenue", DecimalType(18,4), False), StructField("gross_revenue", DecimalType(18,4), False), StructField("return_value", DecimalType(18,4), False), StructField("invoice_count", LongType(), False), StructField("line_count", LongType(), False)]),
 "product_sales": StructType([StructField("stock_code", StringType(), False), StructField("description", StringType(), False), StructField("net_quantity", LongType(), False), StructField("net_revenue", DecimalType(18,4), False), StructField("invoice_count", LongType(), False)]),
 "country_sales": StructType([StructField("country", StringType(), False), StructField("net_revenue", DecimalType(18,4), False), StructField("invoice_count", LongType(), False), StructField("customer_count", LongType(), False)]),
}


def _batches(rows: Iterator, size: int = 1000):
    batch = []
    for row in rows:
        batch.append(tuple(row))
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def load_gold(connection_args: dict, tables: dict[str, DataFrame]) -> None:
    """Legacy full-table upsert for callers with already-built Gold frames."""
    with psycopg2.connect(**connection_args) as conn:
        with conn.cursor() as cursor:
            for table_name, frame in tables.items():
                columns, keys = UPSERTS[table_name]
                names = columns.split(",")
                updates = [c for c in names if c not in keys.split(",")]
                update = ",".join(f"{c}=EXCLUDED.{c}" for c in updates)
                sql = f"INSERT INTO {table_name} ({columns}) VALUES %s ON CONFLICT ({keys}) DO UPDATE SET {update}"
                for batch in _batches(frame.select(*names).toLocalIterator()):
                    execute_values(cursor, sql, batch, page_size=1000)


def _stage_frame(cursor, frame: DataFrame) -> None:
    from pyspark.sql import functions as F
    from .transform import with_line_id

    key = with_line_id(frame)
    if "sale_date" not in key.columns:
        key = key.withColumn("sale_date", F.to_date("invoice_ts"))
    for source_column, target_column, data_type in (
        ("_source_file", "source_file", "string"),
        ("_run_id", "run_id", "string"),
        ("_ingested_at", "ingested_at", "timestamp"),
    ):
        if source_column in key.columns:
            key = key.withColumnRenamed(source_column, target_column)
        else:
            key = key.withColumn(target_column, F.lit(None).cast(data_type))
    staged = key.withColumnRenamed("_source_fingerprint", "source_fingerprint").select(*FACT_COLUMNS)
    sql = f"INSERT INTO line_stage ({','.join(FACT_COLUMNS)}) VALUES %s"
    for batch in _batches(staged.toLocalIterator()):
        execute_values(cursor, sql, batch, page_size=1000)


def _upsert_staged_facts(cursor, conflict: str = "DO UPDATE") -> None:
    updates = ",".join(f"{c}=EXCLUDED.{c}" for c in FACT_COLUMNS if c != "line_id")
    cursor.execute(
        f"INSERT INTO retail_line_items ({','.join(FACT_COLUMNS)}) SELECT {','.join(FACT_COLUMNS)} FROM line_stage WHERE TRUE "
        f"ON CONFLICT (line_id) {conflict}" + (f" SET {updates}" if conflict == "DO UPDATE" else "")
    )


def _rebuild_all_gold(cursor) -> None:
    """One-time migration bootstrap for a legacy Silver snapshot."""
    cursor.execute("DELETE FROM daily_sales")
    cursor.execute("""INSERT INTO daily_sales (sale_date,country,net_revenue,gross_revenue,return_value,invoice_count,line_count)
        SELECT sale_date,country,SUM(line_amount),SUM(CASE WHEN quantity>0 THEN line_amount ELSE 0 END),
        SUM(CASE WHEN quantity<0 THEN line_amount ELSE 0 END),COUNT(DISTINCT invoice_no),COUNT(*)
        FROM retail_line_items GROUP BY sale_date,country""")
    cursor.execute("DELETE FROM product_sales")
    cursor.execute("""INSERT INTO product_sales (stock_code,description,net_quantity,net_revenue,invoice_count)
        SELECT stock_code,description,SUM(quantity),SUM(line_amount),COUNT(DISTINCT invoice_no)
        FROM retail_line_items GROUP BY stock_code,description""")
    cursor.execute("DELETE FROM country_sales")
    cursor.execute("""INSERT INTO country_sales (country,net_revenue,invoice_count,customer_count)
        SELECT country,SUM(line_amount),COUNT(DISTINCT invoice_no),COUNT(DISTINCT customer_id)
        FROM retail_line_items GROUP BY country""")

def _verify_impacted_revenue(cursor) -> None:
    cursor.execute("""
        SELECT
          (SELECT COALESCE(SUM(f.line_amount),0) FROM retail_line_items f JOIN impact_daily i USING(sale_date,country)),
          (SELECT COALESCE(SUM(g.net_revenue),0) FROM daily_sales g JOIN impact_daily i USING(sale_date,country)),
          (SELECT COALESCE(SUM(f.line_amount),0) FROM retail_line_items f JOIN impact_product i USING(stock_code,description)),
          (SELECT COALESCE(SUM(g.net_revenue),0) FROM product_sales g JOIN impact_product i USING(stock_code,description)),
          (SELECT COALESCE(SUM(f.line_amount),0) FROM retail_line_items f JOIN impact_country i USING(country)),
          (SELECT COALESCE(SUM(g.net_revenue),0) FROM country_sales g JOIN impact_country i USING(country))
    """)
    totals = cursor.fetchone()
    if any(totals[index] != totals[index + 1] for index in (0, 2, 4)):
        raise ValueError(f"Warehouse Gold reconciliation failed for impacted keys: {totals}")

def is_batch_processed(connection_args: dict, batch_id: str) -> bool:
    """Check the committed ledger before starting Spark for a forced retry."""
    with psycopg2.connect(**connection_args) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT to_regclass('pipeline_batches')")
            if cursor.fetchone()[0] is None:
                return False
            cursor.execute("SELECT 1 FROM pipeline_batches WHERE batch_id=%s", (batch_id,))
            return cursor.fetchone() is not None

def apply_incremental_batch(connection_args: dict, batch_id: str, source_name: str,
                            frame: DataFrame, baseline: DataFrame | None = None, row_count: int | None = None) -> bool:
    """Atomically upsert changed lines, refresh affected Gold keys, and record batch ID.

    Returns False if this content fingerprint was already applied.
    """
    with psycopg2.connect(**connection_args) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(1577934821)")
            cursor.execute(FACT_DDL)
            cursor.execute("""INSERT INTO pipeline_batches(batch_id,source_name,row_count)
                VALUES (%s,%s,%s) ON CONFLICT(batch_id) DO NOTHING RETURNING batch_id""",
                (batch_id, source_name, row_count if row_count is not None else frame.count()))
            if cursor.fetchone() is None:
                return False

            cursor.execute("SELECT COUNT(*) FROM retail_line_items")
            if cursor.fetchone()[0] == 0 and baseline is not None:
                cursor.execute(STAGE_DDL)
                _stage_frame(cursor, baseline)
                _upsert_staged_facts(cursor, "DO NOTHING")
                _rebuild_all_gold(cursor)
                cursor.execute("DROP TABLE line_stage")

            cursor.execute(STAGE_DDL)
            _stage_frame(cursor, frame)
            # Capture old and new grouping keys before mutating the canonical lines.
            cursor.execute("CREATE TEMP TABLE impact_daily ON COMMIT DROP AS SELECT sale_date,country FROM line_stage UNION SELECT f.sale_date,f.country FROM retail_line_items f JOIN line_stage s USING(line_id)")
            cursor.execute("CREATE TEMP TABLE impact_product ON COMMIT DROP AS SELECT stock_code,description FROM line_stage UNION SELECT f.stock_code,f.description FROM retail_line_items f JOIN line_stage s USING(line_id)")
            cursor.execute("CREATE TEMP TABLE impact_country ON COMMIT DROP AS SELECT country FROM line_stage UNION SELECT f.country FROM retail_line_items f JOIN line_stage s USING(line_id)")
            _upsert_staged_facts(cursor)
            # Refresh only affected dimension keys; counts-distinct remain exact.
            cursor.execute("DELETE FROM daily_sales d USING impact_daily i WHERE d.sale_date=i.sale_date AND d.country=i.country")
            cursor.execute("""INSERT INTO daily_sales (sale_date,country,net_revenue,gross_revenue,return_value,invoice_count,line_count)
                SELECT f.sale_date,f.country,SUM(f.line_amount),SUM(CASE WHEN f.quantity>0 THEN f.line_amount ELSE 0 END),
                SUM(CASE WHEN f.quantity<0 THEN f.line_amount ELSE 0 END),COUNT(DISTINCT f.invoice_no),COUNT(*)
                FROM retail_line_items f JOIN impact_daily i USING(sale_date,country) GROUP BY f.sale_date,f.country""")
            cursor.execute("DELETE FROM product_sales p USING impact_product i WHERE p.stock_code=i.stock_code AND p.description=i.description")
            cursor.execute("""INSERT INTO product_sales (stock_code,description,net_quantity,net_revenue,invoice_count)
                SELECT f.stock_code,f.description,SUM(f.quantity),SUM(f.line_amount),COUNT(DISTINCT f.invoice_no)
                FROM retail_line_items f JOIN impact_product i USING(stock_code,description) GROUP BY f.stock_code,f.description""")
            cursor.execute("DELETE FROM country_sales c USING impact_country i WHERE c.country=i.country")
            cursor.execute("""INSERT INTO country_sales (country,net_revenue,invoice_count,customer_count)
                SELECT f.country,SUM(f.line_amount),COUNT(DISTINCT f.invoice_no),COUNT(DISTINCT f.customer_id)
                FROM retail_line_items f JOIN impact_country i USING(country) GROUP BY f.country""")
            _verify_impacted_revenue(cursor)
    return True


def rebuild_warehouse_state(connection_args: dict, batch_id: str, source_name: str,
                            frame: DataFrame, row_count: int) -> None:
    """Atomically replace current facts and Gold after an explicit backfill.

    This intentionally performs a full rebuild: backfills may change historical
    rows outside the newest batch's affected keys. The transaction rolls back
    the truncate, fact insert, Gold refresh, and ledger update together.
    """
    with psycopg2.connect(**connection_args) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(1577934821)")
            cursor.execute(FACT_DDL)
            cursor.execute(STAGE_DDL)
            _stage_frame(cursor, frame)
            cursor.execute("TRUNCATE TABLE retail_line_items")
            cursor.execute(
                f"INSERT INTO retail_line_items ({','.join(FACT_COLUMNS)}) "
                f"SELECT {','.join(FACT_COLUMNS)} FROM line_stage"
            )
            _rebuild_all_gold(cursor)
            cursor.execute(
                "INSERT INTO pipeline_batches(batch_id,source_name,row_count) VALUES (%s,%s,%s) "
                "ON CONFLICT(batch_id) DO UPDATE SET source_name=EXCLUDED.source_name, "
                "row_count=EXCLUDED.row_count, applied_at=now()",
                (batch_id, source_name, row_count),
            )


def read_gold_tables(spark: SparkSession, connection_args: dict) -> dict[str, DataFrame]:
    """Read compact current Gold tables so their Parquet copies can be refreshed."""
    result = {}
    with psycopg2.connect(**connection_args) as conn:
        with conn.cursor() as cursor:
            for table, schema in GOLD_SCHEMAS.items():
                cursor.execute(f"SELECT * FROM {table}")
                result[table] = spark.createDataFrame(cursor.fetchall(), schema=schema)
    return result
