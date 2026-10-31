from pyspark.sql import DataFrame, functions as F


def standardize(frame: DataFrame) -> DataFrame:
    return (frame
        .withColumn("invoice_no", F.trim(F.col("InvoiceNo").cast("string")))
        .withColumn("stock_code", F.trim(F.col("StockCode").cast("string")))
        .withColumn("description", F.coalesce(F.trim(F.col("Description").cast("string")), F.lit("<UNKNOWN>")))
        .withColumn("quantity", F.col("Quantity").cast("long"))
        .withColumn("invoice_ts", F.to_timestamp("InvoiceDate"))
        .withColumn("unit_price", F.col("UnitPrice").cast("decimal(12, 4)"))
        .withColumn("customer_id", F.col("CustomerID").cast("long"))
        .withColumn("country", F.trim(F.col("Country").cast("string")))
        .withColumn("is_return", F.col("Quantity").cast("long") < 0)
        .withColumn("is_cancellation", F.upper(F.col("InvoiceNo").cast("string")).startswith("C")))


def validate(frame: DataFrame) -> tuple[DataFrame, DataFrame]:
    quantity_text = F.trim(F.col("Quantity").cast("string"))
    customer_text = F.trim(F.col("CustomerID").cast("string"))
    integer_text = r"^[+-]?\d+(?:\.0+)?$"
    required = [
        F.when(F.col("invoice_no").isNull() | (F.col("invoice_no") == ""), F.lit("missing_invoice_no")),
        F.when(F.col("stock_code").isNull() | (F.col("stock_code") == ""), F.lit("missing_stock_code")),
        F.when(F.col("invoice_ts").isNull(), F.lit("invalid_invoice_date")),
        F.when(F.col("quantity").isNull() | (F.col("quantity") == 0) | ~quantity_text.rlike(integer_text), F.lit("invalid_quantity")),
        F.when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), F.lit("invalid_unit_price")),
        F.when(F.col("country").isNull() | (F.col("country") == ""), F.lit("missing_country")),
        F.when((customer_text != "") & (F.col("customer_id").isNull() | ~customer_text.rlike(integer_text)), F.lit("invalid_customer_id")),
        F.when(F.col("quantity").isNotNull() & F.col("unit_price").isNotNull() & (F.col("quantity") * F.col("unit_price")).cast("decimal(18, 4)").isNull(), F.lit("line_amount_overflow")),
    ]
    checked = frame
    if "_input_line_id" in frame.columns:
        has_id = F.col("_input_line_id").isNotNull() & (F.trim(F.col("_input_line_id")) != "")
        payload = F.to_json(F.struct(*[F.col(c).alias(c) for c in [
            "invoice_no", "stock_code", "description", "quantity", "invoice_ts",
            "unit_price", "customer_id", "country",
        ]]))
        conflicts = (frame.filter(has_id).groupBy("_input_line_id")
            .agg(F.countDistinct(payload).alias("_payload_versions"))
            .filter(F.col("_payload_versions") > 1)
            .select("_input_line_id").withColumn("_line_id_conflict", F.lit(True)))
        checked = checked.join(conflicts, on="_input_line_id", how="left")
        required.append(F.when(F.coalesce(F.col("_line_id_conflict"), F.lit(False)), F.lit("duplicate_line_id_conflict")))
    checked = checked.withColumn("_rejection_reasons", F.concat_ws("|", *required))
    return checked.filter(F.col("_rejection_reasons") == ""), checked.filter(F.col("_rejection_reasons") != "")
