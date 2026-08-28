from pyspark.sql.types import StructType, StructField, StringType

# Keep source columns as strings in Bronze -> casting and validation happen later
products_schema = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", StringType(), True),
        StructField("stock_quantity", StringType(), True),
    ]
)
