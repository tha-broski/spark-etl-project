from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType, DateType


def transform_customers(bronze_df):
    # Transform Bronze columns into Silver types and trim strings | used in main.py
    silver_df = bronze_df.withColumn(
        "customer_id", f.col("customer_id").cast(IntegerType())
    )
    silver_df = silver_df.withColumn("first_name", f.trim(f.col("first_name")))
    silver_df = silver_df.withColumn("last_name", f.trim(f.col("last_name")))
    silver_df = silver_df.withColumn("email", f.trim(f.col("email")))
    silver_df = silver_df.withColumn("country", f.trim(f.col("country")))
    silver_df = silver_df.withColumn(
        "registration_date", f.col("registration_date").cast(DateType())
    )
    # Mark customers from snapshot as active | loading/silver.py can reactivate or soft-delete products
    silver_df = silver_df.withColumn("is_active", f.lit(True))
    return silver_df
