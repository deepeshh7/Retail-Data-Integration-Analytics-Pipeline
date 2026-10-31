from pyspark.sql import DataFrame, functions as F

# UCI Online Retail has no line ID. For it, the full business event payload is
# the stable key. Inputs with an immutable LineID use that key for updates.
EVENT_KEY_COLUMNS = ["invoice_no", "stock_code", "description", "quantity", "invoice_ts",
                     "unit_price", "customer_id", "country"]


def with_line_id(frame: DataFrame) -> DataFrame:
    event_key = F.sha2(F.to_json(F.struct(*[F.col(name).alias(name) for name in EVENT_KEY_COLUMNS])), 256)
    if "_input_line_id" in frame.columns:
        supplied = F.trim(F.col("_input_line_id"))
        line_id = F.when(supplied.isNotNull() & (supplied != ""), F.sha2(supplied, 256)).otherwise(event_key)
    else:
        line_id = event_key
    return frame.withColumn("line_id", line_id)


def silver(accepted: DataFrame) -> DataFrame:
    columns = [
        "invoice_no", "stock_code", "description", "quantity", "invoice_ts", "unit_price", "customer_id", "country",
        "is_return", "is_cancellation", "_source_file", "_source_fingerprint", "_ingested_at",
    ]
    if "_input_line_id" in accepted.columns:
        columns.append("_input_line_id")
    if "_run_id" in accepted.columns:
        columns.append("_run_id")
    # Preserve additive source fields without assigning them business meaning.
    # A new field (for example Discount) requires an explicit transformation
    # decision before it can affect line_amount or analytics.
    for column in ("_source_extras", "_source_extra_columns", "_source_schema_version"):
        if column in accepted.columns:
            columns.append(column)
    selected = accepted.select(*columns)
    return (with_line_id(selected).dropDuplicates(["line_id"])
        .withColumn("line_amount", (F.col("quantity") * F.col("unit_price")).cast("decimal(18, 4)"))
        .withColumn("sale_date", F.to_date("invoice_ts")))


def gold(silver_df: DataFrame) -> dict[str, DataFrame]:
    daily = silver_df.groupBy("sale_date", "country").agg(
        F.sum("line_amount").alias("net_revenue"),
        F.sum(F.when(F.col("quantity") > 0, F.col("line_amount")).otherwise(F.lit(0))).alias("gross_revenue"),
        F.sum(F.when(F.col("quantity") < 0, F.col("line_amount")).otherwise(F.lit(0))).alias("return_value"),
        F.countDistinct("invoice_no").alias("invoice_count"), F.count("line_id").alias("line_count"))
    products = silver_df.groupBy("stock_code", "description").agg(
        F.sum("quantity").alias("net_quantity"), F.sum("line_amount").alias("net_revenue"),
        F.countDistinct("invoice_no").alias("invoice_count"))
    countries = silver_df.groupBy("country").agg(
        F.sum("line_amount").alias("net_revenue"), F.countDistinct("invoice_no").alias("invoice_count"),
        F.countDistinct("customer_id").alias("customer_count"))
    return {"daily_sales": daily, "product_sales": products, "country_sales": countries}
