from config.spark_session import create_spark_session
from ingestion.bronze import ingest_to_bronze
from schemas.ecommerce import products_schema
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

spark = create_spark_session()
logger.info("Pipeline started")
ingest_to_bronze(
    spark, "data/raw/products.csv", products_schema, "data/bronze/products"
)
logger.info("Pipeline completed successfully")
spark.stop()
