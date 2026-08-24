import logging

from config.spark_session import create_spark_session
from ingestion.bronze import ingest_to_bronze
from transformations.products import transform_products
from schemas.ecommerce import products_schema

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

silver_df = transform_products(bronze_df)

silver_df.printSchema()

logger.info("Pipeline completed successfully")

spark.stop()
