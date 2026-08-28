import logging

from config.spark_session import create_spark_session
from pyspark.sql import functions as f
from ingestion.bronze import ingest_to_bronze
from transformations.products import transform_products
from schemas.ecommerce import products_schema
from quality.products import validate_products
from utils.file_utils import calculate_file_hash
from control.processed_files import mark_file_status, get_file_state
from loading.silver import load_products_to_silver
from loading.quarantine import load_products_to_quarantine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

SOURCE_PATH = "data/raw/products.csv"
BRONZE_PATH = "data/bronze/products"
SILVER_PATH = "data/silver/products"
QUARANTINE_PATH = "data/quarantine/products"
CONTROL_PATH = "data/control/processed_files"

logger = logging.getLogger(__name__)


def main():
    spark = create_spark_session()

    logger.info("Pipeline started")

    file_hash = calculate_file_hash(SOURCE_PATH)
    file_state = get_file_state(spark, file_hash, CONTROL_PATH)

    if file_state is not None and file_state.status == "SUCCESS":
        logger.info("File is already processed and will be skipped")
        spark.stop()
        return
    elif file_state is not None and file_state.status == "BRONZE_WRITTEN":
        batch_id = file_state.batch_id
    elif file_state is None:
        batch_id = ingest_to_bronze(
            spark,
            SOURCE_PATH,
            products_schema,
            BRONZE_PATH,
        )
        mark_file_status(
            spark, SOURCE_PATH, file_hash, batch_id, "BRONZE_WRITTEN", CONTROL_PATH
        )

    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    current_batch_df = bronze_df.filter(f.col("batch_id") == batch_id)

    logger.info("Silver transformation started")
    silver_df = transform_products(current_batch_df)

    validated_df = validate_products(silver_df).cache()
    snapshot_product_ids = validated_df.select("product_id")
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
        load_products_to_quarantine(spark, invalid_df, QUARANTINE_PATH)
        load_products_to_silver(spark, valid_df, SILVER_PATH, snapshot_product_ids)
        logger.info("Silver and Quarantine data saved successfully")
    except Exception:
        logger.exception("Silver transformation failed")
        raise
    finally:
        validated_df.unpersist()

    mark_file_status(spark, SOURCE_PATH, file_hash, batch_id, "SUCCESS", CONTROL_PATH)
    logger.info("Pipeline completed successfully")

    spark.stop()


if __name__ == "__main__":
    main()
