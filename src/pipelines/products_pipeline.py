import logging

from pyspark.sql import functions as f

from control.processed_files import mark_file_status, prepare_file_batch
from loading.quarantine import load_to_quarantine
from loading.silver import load_snapshot_to_silver
from quality.products import validate_products
from schemas.ecommerce import products_schema
from transformations.products import transform_products

logger = logging.getLogger(__name__)


def process_products(
    spark,
    source_path,
    bronze_path,
    silver_path,
    quarantine_path,
    control_path,
):
    file_hash, batch_id, should_skip = prepare_file_batch(
        spark,
        source_path,
        products_schema,
        bronze_path,
        control_path,
    )
    if should_skip:
        return

    # Read Bronze and keep only current batch_id because Bronze can contain historical snapshots

    bronze_df = spark.read.format("delta").load(bronze_path)

    current_batch_df = bronze_df.filter(f.col("batch_id") == batch_id)

    logger.info("Products Silver transformation started")

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

    # Split valid rows to Silver and invalid rows to Quarantine
    valid_df = validated_df.filter(f.col("is_valid"))
    invalid_df = validated_df.filter(~f.col("is_valid"))

    try:
        # loading/quarantine.py -> idempotent writing (by batch_id + product_id) of invalid records to Quarantine
        load_to_quarantine(
            spark,
            invalid_df,
            quarantine_path,
            "product_id",
        )

        # Snapshot load: insert new, update changed, reactivate returning, soft-delete missing
        load_snapshot_to_silver(
            spark,
            valid_df,
            silver_path,
            snapshot_product_ids,
            "product_id",
            [
                "name",
                "category",
                "price",
                "stock_quantity",
                "is_active",
            ],
        )

        logger.info("Products Silver and Quarantine data saved successfully")

    except Exception:
        # Silver or Quarantine write failed -> no success | control stays at BRONZE_WRITTEN | retry will start from existing bronze batch
        logger.exception("Products Silver transformation failed")
        raise

    finally:
        # Remove the cached DataFrame
        validated_df.unpersist()

    # Mark batch as SUCCESS so future runs for the same file hash are skipped
    mark_file_status(
        spark,
        source_path,
        file_hash,
        batch_id,
        "SUCCESS",
        control_path,
    )
