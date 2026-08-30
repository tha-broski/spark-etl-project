import logging

from pyspark.sql import functions as f

from control.processed_files import mark_file_status, prepare_file_batch
from loading.quarantine import load_to_quarantine
from loading.silver import load_incremental_to_silver
from quality.order_items import validate_order_items
from schemas.ecommerce import order_items_schema
from transformations.order_items import transform_order_items

logger = logging.getLogger(__name__)


def process_order_items(
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
        order_items_schema,
        bronze_path,
        control_path,
    )

    if should_skip:
        return

    # Read Bronze and keep only the current batch
    bronze_df = spark.read.format("delta").load(bronze_path)

    current_batch_df = bronze_df.filter(f.col("batch_id") == batch_id)

    logger.info("Order Items Silver transformation started")

    silver_df = transform_order_items(current_batch_df)

    validated_df = validate_order_items(silver_df).cache()

    total_rows = validated_df.count()
    valid_rows = validated_df.filter(f.col("is_valid")).count()
    invalid_rows = validated_df.filter(~f.col("is_valid")).count()

    logger.info(
        "Order Items validation completed | total rows: %s | valid rows: %s | invalid rows: %s",
        total_rows,
        valid_rows,
        invalid_rows,
    )

    # Split valid rows to Silver and invalid rows to Quarantine
    valid_df = validated_df.filter(f.col("is_valid"))
    invalid_df = validated_df.filter(~f.col("is_valid"))

    try:
        load_to_quarantine(
            spark,
            invalid_df,
            quarantine_path,
            "order_item_id",
        )

        # Incremental load: insert new and update changed items.
        # Items missing from the current batch remain unchanged.
        load_incremental_to_silver(
            spark,
            valid_df,
            silver_path,
            "order_item_id",
            [
                "order_id",
                "product_id",
                "quantity",
                "unit_price",
            ],
        )

        logger.info("Order Items Silver and Quarantine data saved successfully")

    except Exception:
        # No SUCCESS on failure; retry will reuse the existing Bronze batch
        logger.exception("Order Items Silver transformation failed")
        raise

    finally:
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
