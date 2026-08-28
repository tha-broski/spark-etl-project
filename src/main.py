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
    # config/spark_session.py -> Spark and delta config
    spark = create_spark_session()

    logger.info("Pipeline started")
    # utils/file_utils.py -> source file hashing logic used to identify the file content, used later to verify if the file has already been processed
    file_hash = calculate_file_hash(SOURCE_PATH)
    # control/processed_files.py -> Control state logic | verify if the file has been processed (New file/Bronze written/Success)
    file_state = get_file_state(spark, file_hash, CONTROL_PATH)

    # file processed -> skip the file and stop the pipeline | Prevents duplicates
    if file_state is not None and file_state.status == "SUCCESS":
        logger.info("File is already processed and will be skipped")
        spark.stop()
        return

    # Bronze written then fail -> reuse batch_id from control | prevents appending same stuff to the bronze twice
    elif file_state is not None and file_state.status == "BRONZE_WRITTEN":
        batch_id = file_state.batch_id

    # File hasn't been processed yet -> append it to Bronze | batch_id generated in ingestion/bronze.py
    elif file_state is None:
        batch_id = ingest_to_bronze(
            spark,
            SOURCE_PATH,
            products_schema,
            BRONZE_PATH,
        )

        # Update the status to "BRONZE_WRITTEN" in control/processed_files.py | allows to avoid duplicates if later stage fails
        mark_file_status(
            spark, SOURCE_PATH, file_hash, batch_id, "BRONZE_WRITTEN", CONTROL_PATH
        )

    # Read Bronze and keep only current batch_id because Bronze can contain historical snapshots
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    current_batch_df = bronze_df.filter(f.col("batch_id") == batch_id)

    logger.info("Silver transformation started")

    # transformations/products.py -> transforming/casting data from bronze to silver
    silver_df = transform_products(current_batch_df)

    # quality/products.py -> Validate and check data quality before loading to silver
    # cache() used because validated_df is reused below
    validated_df = validate_products(silver_df).cache()

    # store the product IDs from current snapshot (including invalid rows) | used in loading/silver.py to detect missing products
    snapshot_product_ids = validated_df.select("product_id")

    # counting total/valid/invalid rows for logging and observability
    total_rows = validated_df.count()
    valid_rows = validated_df.filter(f.col("is_valid")).count()
    invalid_rows = validated_df.filter(~f.col("is_valid")).count()

    logger.info(
        "Products validation completed | total rows: %s | valid rows: %s | invalid rows: %s",
        total_rows,
        valid_rows,
        invalid_rows,
    )

    # after validation and quality check split the data into valid->Silver and invalid->Quarantine
    valid_df = validated_df.filter(f.col("is_valid"))
    invalid_df = validated_df.filter(~f.col("is_valid"))

    try:
        # loading/quarantine.py -> idempotent writing (by batch_id + product_id) of invalid records to Quarantine
        load_products_to_quarantine(spark, invalid_df, QUARANTINE_PATH)

        # loading/silver.py | New Product -> Insert | Changed Product -> Update | Inactive Product -> Reactivate (if needed) | Missing Product -> soft-delete
        load_products_to_silver(spark, valid_df, SILVER_PATH, snapshot_product_ids)
        logger.info("Silver and Quarantine data saved successfully")
    except Exception:
        # Silver or Quarantine write failed -> no success | control stays at BRONZE_WRITTEN | retry will start from existing bronze batch
        logger.exception("Silver transformation failed")
        raise
    finally:
        # Remove the cached DataFrame
        validated_df.unpersist()

    # Everything went well -> write state as "SUCCESS" to control/processed_files.py | future runs for same hash will be skipped
    mark_file_status(spark, SOURCE_PATH, file_hash, batch_id, "SUCCESS", CONTROL_PATH)
    logger.info("Pipeline completed successfully")

    spark.stop()


if __name__ == "__main__":
    main()
