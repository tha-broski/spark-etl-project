from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType, DecimalType


def transform_order_items(bronze_df):
    # Transform Bronze columns into Silver-ready types
    silver_df = bronze_df.withColumn(
        "order_item_id", f.col("order_item_id").cast(IntegerType())
    )

    silver_df = silver_df.withColumn("order_id", f.col("order_id").cast(IntegerType()))

    silver_df = silver_df.withColumn(
        "product_id", f.col("product_id").cast(IntegerType())
    )

    silver_df = silver_df.withColumn("quantity", f.col("quantity").cast(IntegerType()))

    silver_df = silver_df.withColumn(
        "unit_price", f.col("unit_price").cast(DecimalType(10, 2))
    )

    return silver_df
