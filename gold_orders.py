from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def rank_by_revenue(df):
    w = Window.orderBy(F.col("total_revenue").desc())
    return df.withColumn("revenue_rank", F.dense_rank().over(w))


def sales_by(column):
    df = spark.read.table("gold_delivered_orders")
    out = df.groupBy(column).agg(
        F.countDistinct("order_id").alias("total_delivered_orders"),
        F.sum("quantity").alias("total_units_sold"),
        F.sum("order_amount").alias("total_revenue"),
    )
    out = out.withColumn(
        "average_order_value",
        F.round(F.col("total_revenue") / F.col("total_delivered_orders"), 2),
    )
    return rank_by_revenue(out)


@dp.materialized_view(
    name="gold_delivered_orders",
    comment="Delivered orders only, used for all sales and revenue analysis"
)
def gold_delivered_orders():
    return spark.read.table("silver_orders").filter(
        F.col("order_status") == "Delivered"
    )


@dp.materialized_view(
    name="gold_city_sales",
    comment="City-wise delivered orders, units and revenue"
)
def gold_city_sales():
    return sales_by("city")


@dp.materialized_view(
    name="gold_category_sales",
    comment="Category-wise delivered orders, units and revenue"
)
def gold_category_sales():
    return sales_by("category")


@dp.materialized_view(
    name="gold_product_sales",
    comment="Product-wise delivered orders, units and revenue"
)
def gold_product_sales():
    return sales_by("product_name")


@dp.materialized_view(
    name="gold_payment_analysis",
    comment="Payment-mode-wise delivered orders and revenue"
)
def gold_payment_analysis():
    out = sales_by("payment_mode").withColumnRenamed(
        "average_order_value", "average_transaction_value"
    )
    total = F.sum("total_revenue").over(Window.partitionBy())
    return out.withColumn(
        "revenue_contribution_pct",
        F.round(F.col("total_revenue") * 100 / total, 2),
    )


@dp.materialized_view(
    name="gold_order_summary",
    comment="Order count by status across all valid orders"
)
def gold_order_summary():
    df = spark.read.table("silver_orders")
    out = df.groupBy("order_status").agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("order_amount").alias("total_order_value"),
    )
    total = F.sum("total_orders").over(Window.partitionBy())
    return out.withColumn(
        "order_share_pct",
        F.round(F.col("total_orders") * 100 / total, 2),
    )


@dp.materialized_view(
    name="gold_sales_summary",
    comment="Overall KPIs: delivered sales and order lifecycle rates"
)
def gold_sales_summary():
    df = spark.read.table("silver_orders")
    is_delivered = F.col("order_status") == "Delivered"

    def count_status(s):
        return F.sum(F.when(F.col("order_status") == s, 1).otherwise(0))

    out = df.agg(
        F.countDistinct("order_id").alias("total_orders"),
        count_status("Delivered").alias("delivered_orders"),
        count_status("Shipped").alias("shipped_orders"),
        count_status("Processing").alias("processing_orders"),
        count_status("Cancelled").alias("cancelled_orders"),
        count_status("Returned").alias("returned_orders"),
        F.sum(F.when(is_delivered, F.col("order_amount"))).alias("total_revenue"),
        F.sum(F.when(is_delivered, F.col("quantity"))).alias("units_sold"),
    )
    return (
        out
        .withColumn(
            "average_order_value",
            F.round(F.col("total_revenue") / F.col("delivered_orders"), 2),
        )
        .withColumn(
            "delivery_rate_pct",
            F.round(F.col("delivered_orders") * 100 / F.col("total_orders"), 2),
        )
        .withColumn(
            "cancellation_rate_pct",
            F.round(F.col("cancelled_orders") * 100 / F.col("total_orders"), 2),
        )
    )
