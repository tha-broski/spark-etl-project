from pyspark.sql import functions as f
from pyspark.sql.types import DecimalType


def build_daily_sales(orders_df, order_items_df):
    completed_orders_df = orders_df.filter(f.col("status") == "completed")

    # innter join just the orders that have item
    sales_df = completed_orders_df.join(
        order_items_df,
        on="order_id",
        how="inner",
    )

    # Calculate the revenue for single sale
    sales_df = sales_df.withColumn(
        "line_revenue",
        f.col("quantity") * f.col("unit_price"),
    )

    # Group sales by day
    daily_sales_df = sales_df.groupBy("order_date").agg(
        f.countDistinct("order_id").alias("order_count"),
        f.sum("quantity").alias("items_sold"),
        f.sum("line_revenue").alias("revenue"),
    )

    # Keep financial metrics on a stable schema
    daily_sales_df = daily_sales_df.withColumn(
        "revenue",
        f.col("revenue").cast(DecimalType(18, 2)),
    )

    daily_sales_df = daily_sales_df.withColumn(
        "average_order_value",
        (f.col("revenue") / f.col("order_count")).cast(DecimalType(18, 2)),
    )

    return daily_sales_df
