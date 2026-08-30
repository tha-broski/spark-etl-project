import logging

from pyspark.sql import functions as f

from control.processed_files import mark_file_status, prepare_file_batch
from loading.quarantine import load_to_quarantine
from loading.silver import load_incremental_to_silver
from quality.orders import validate_orders
from schemas.ecommerce import orders_schema
from transformations.orders import transform_orders

logger = logging.getLogger(__name__)


def process_orders(
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
        orders_schema,
        bronze_path,
        control_path,
    )

    if should_skip:
        return

    # Read Bronze and keep only the current batch
    bronze_df = spark.read.format("delta").load(bronze_path)

    current_batch_df = bronze_df.filter(f.col("batch_id") == batch_id)

    logger.info("Orders Silver transformation started")

    silver_df = transform_orders(current_batch_df)

    validated_df = validate_orders(silver_df).cache()

    total_rows = validated_df.count()
    valid_rows = validated_df.filter(f.col("is_valid")).count()
    invalid_rows = validated_df.filter(~f.col("is_valid")).count()

    logger.info(
        "Orders validation completed | total rows: %s | valid rows: %s | invalid rows: %s",
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
            "order_id",
        )

        # Incremental load: insert new and update changed orders.
        # Orders missing from the current batch remain unchanged.
        load_incremental_to_silver(
            spark,
            valid_df,
            silver_path,
            "order_id",
            [
                "customer_id",
                "order_date",
                "status",
            ],
        )

        logger.info("Orders Silver and Quarantine data saved successfully")

    except Exception:
        # No SUCCESS on failure; retry will reuse the existing Bronze batch
        logger.exception("Orders Silver transformation failed")
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
