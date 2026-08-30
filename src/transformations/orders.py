from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType, DateType


def transform_orders(bronze_df):
    # Transform Bronze columns into Silver types and trim strings | used in main.py
    silver_df = bronze_df.withColumn("order_id", f.col("order_id").cast(IntegerType()))
    silver_df = silver_df.withColumn(
        "customer_id", f.col("customer_id").cast(IntegerType())
    )
    silver_df = silver_df.withColumn("order_date", f.col("order_date").cast(DateType()))
    silver_df = silver_df.withColumn("status", f.trim(f.col("status")))

    return silver_df
