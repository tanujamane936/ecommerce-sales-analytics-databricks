from pyspark import pipelines as dp
from pyspark.sql import functions as F

TEXT_COLS = [
    "order_id", "order_date", "customer_id", "customer_name", "city",
    "product_name", "category", "quantity", "unit_price",
    "payment_mode", "order_status",
]
VALID_STATUS = ["Delivered", "Shipped", "Processing", "Cancelled", "Returned"]


@dp.materialized_view(
    name="silver_orders_cleaned",
    comment="Trimmed, blank-to-NULL and standardized order records"
)
def silver_orders_cleaned():
    df = spark.read.table("bronze_orders_sdp")
    for c in TEXT_COLS:
        df = df.withColumn(c, F.regexp_replace(F.trim(F.col(c)), r"\s+", " "))
        df = df.withColumn(c, F.when(F.col(c) == "", None).otherwise(F.col(c)))
    df = (
        df
        .withColumn("customer_name", F.initcap("customer_name"))
        .withColumn("city", F.initcap("city"))
        .withColumn(
            "product_name",
            F.regexp_replace(F.initcap("product_name"), "(?i)usb-c", "USB-C"),
        )
        .withColumn("category", F.initcap("category"))
        .withColumn(
            "payment_mode",
            F.when(F.upper("payment_mode") == "UPI", "UPI")
             .otherwise(F.initcap("payment_mode")),
        )
        .withColumn("order_status", F.initcap("order_status"))
    )
    return df.select(
        *TEXT_COLS, "ingestion_timestamp", "source_file_name"
    )


@dp.materialized_view(
    name="silver_orders_typed",
    comment="Typed records with order_amount, duplicates removed"
)
def silver_orders_typed():
    df = spark.read.table("silver_orders_cleaned")
    parsed_date = F.coalesce(
        F.expr("try_to_timestamp(order_date, 'yyyy-MM-dd')"),
        F.expr("try_to_timestamp(order_date, 'dd-MM-yyyy')"),
        F.expr("try_to_timestamp(order_date, 'MM/dd/yyyy')"),
    ).cast("date")
    df = (
        df
        .withColumn("order_date", parsed_date)
        .withColumn("quantity", F.expr("try_cast(quantity AS INT)"))
        .withColumn("unit_price", F.expr("try_cast(unit_price AS DECIMAL(12,2))"))
    )
    df = df.withColumn(
        "order_amount",
        (F.col("quantity").cast("decimal(12,2)") * F.col("unit_price"))
        .cast("decimal(18,2)"),
    )
    return df.dropDuplicates(["order_id"])


def add_reasons(df):
    checks = [
        F.when(F.col("order_id").isNull(), F.lit("R1: order_id missing")),
        F.when(F.col("order_date").isNull(), F.lit("R2: order_date invalid")),
        F.when(
            F.col("quantity").isNull() | (F.col("quantity") <= 0),
            F.lit("R3: quantity must be > 0"),
        ),
        F.when(
            F.col("unit_price").isNull() | (F.col("unit_price") <= 0),
            F.lit("R4: unit_price must be > 0"),
        ),
    ]
    for c in ["customer_id", "product_name", "category", "city", "payment_mode"]:
        checks.append(F.when(F.col(c).isNull(), F.lit(f"R5: {c} missing")))
    checks.append(
        F.when(
            F.col("order_status").isNull()
            | (~F.col("order_status").isin(VALID_STATUS)),
            F.lit("R6: order_status invalid"),
        )
    )
    return df.withColumn("rejection_reason", F.concat_ws("; ", *checks))


@dp.materialized_view(
    name="silver_orders",
    comment="Valid, cleaned and typed orders"
)
def silver_orders():
    df = add_reasons(spark.read.table("silver_orders_typed"))
    return df.filter(F.col("rejection_reason") == "").drop("rejection_reason")


@dp.materialized_view(
    name="silver_orders_rejected",
    comment="Orders that failed validation, with reasons"
)
def silver_orders_rejected():
    df = add_reasons(spark.read.table("silver_orders_typed"))
    return df.filter(F.col("rejection_reason") != "")
