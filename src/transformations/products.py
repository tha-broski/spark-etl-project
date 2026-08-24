from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType, DecimalType


def transform_products(bronze_df):
    silver_df = bronze_df.withColumn(
        "product_id", f.col("product_id").cast(IntegerType())
    )
    silver_df = silver_df.withColumn("name", f.trim(f.col("name")))
    silver_df = silver_df.withColumn("category", f.trim(f.col("category")))
    silver_df = silver_df.withColumn("price", f.col("price").cast(DecimalType(10, 2)))
    silver_df = silver_df.withColumn(
        "stock_quantity", f.col("stock_quantity").cast(IntegerType())
    )
    return silver_df
