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

customers_schema = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("registration_date", StringType(), True),
    ]
)

orders_schema = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

order_items_schema = StructType(
    [
        StructField("order_item_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_price", StringType(), True),
    ]
)
