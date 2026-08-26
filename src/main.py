import logging

from config.spark_session import create_spark_session
from pyspark.sql import functions as f
from ingestion.bronze import ingest_to_bronze
from transformations.products import transform_products
from schemas.ecommerce import products_schema
from quality.products import validate_products

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


spark = create_spark_session()

logger.info("Pipeline started")

ingest_to_bronze(
    spark,
    "data/raw/products.csv",
    products_schema,
    "data/bronze/products",
)

bronze_df = spark.read.format("delta").load("data/bronze/products")

logger.info("Silver transformation started")
silver_df = transform_products(bronze_df)

validated_df = validate_products(silver_df).cache()
total_rows = validated_df.count()
valid_rows = validated_df.filter(f.col("is_valid")).count()
invalid_rows = validated_df.filter(~f.col("is_valid")).count()

logger.info(
    "Products validation completed | total rows: %s | valid rows: %s | invalid rows: %s",
    total_rows,
    valid_rows,
    invalid_rows,
)
valid_df = validated_df.filter(f.col("is_valid"))
invalid_df = validated_df.filter(~f.col("is_valid"))

try:
    invalid_df.write.format("delta").mode("append").save("data/quarantine/products")
    valid_df.write.format("delta").mode("append").save("data/silver/products")
    logger.info("Silver and Quarantine data saved successfully")
except Exception:
    logger.exception("Silver transformation failed")
    raise
finally:
    validated_df.unpersist()

logger.info("Pipeline completed successfully")

spark.stop()
