from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType, DecimalType


def transform_products(bronze_df):
    # Transform Bronze columns into Silver types and trim strings | used in main.py
    silver_df = bronze_df.withColumn(
        "product_id", f.col("product_id").cast(IntegerType())
    )
    silver_df = silver_df.withColumn("name", f.trim(f.col("name")))
    silver_df = silver_df.withColumn("category", f.trim(f.col("category")))
    silver_df = silver_df.withColumn("price", f.col("price").cast(DecimalType(10, 2)))
    silver_df = silver_df.withColumn(
        "stock_quantity", f.col("stock_quantity").cast(IntegerType())
    )
    # Mark products from snapshot as active | loading/silver.py can reactivate or soft-delete products
    silver_df = silver_df.withColumn("is_active", f.lit(True))
    return silver_df
