from config.spark_session import create_spark_session
from ingestion.bronze import ingest_to_bronze
from schemas.ecommerce import products_schema

spark = create_spark_session()
ingest_to_bronze(
    spark, "data/raw/products.csv", products_schema, "data/bronze/products"
)
spark.stop()
