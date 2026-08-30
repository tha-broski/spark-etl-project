import logging

from pyspark.sql import functions as f

from control.processed_files import mark_file_status, prepare_file_batch
from loading.quarantine import load_to_quarantine
from loading.silver import load_snapshot_to_silver
from quality.customers import validate_customers
from schemas.ecommerce import customers_schema
from transformations.customers import transform_customers

logger = logging.getLogger(__name__)


def process_customers(
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
        customers_schema,
        bronze_path,
        control_path,
    )

    if should_skip:
        return

    # Read Bronze and keep only the current batch
    bronze_df = spark.read.format("delta").load(bronze_path)

    current_batch_df = bronze_df.filter(f.col("batch_id") == batch_id)

    logger.info("Customers Silver transformation started")

    silver_df = transform_customers(current_batch_df)

    validated_df = validate_customers(silver_df).cache()

    # Keep all current snapshot IDs, including invalid rows,
    # so invalid customers are not incorrectly soft-deleted
    snapshot_customer_ids = validated_df.select("customer_id")

    total_rows = validated_df.count()
    valid_rows = validated_df.filter(f.col("is_valid")).count()
    invalid_rows = validated_df.filter(~f.col("is_valid")).count()

    logger.info(
        "Customers validation completed | total rows: %s | valid rows: %s | invalid rows: %s",
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
            "customer_id",
        )

        # Snapshot load: insert new, update changed,
        # reactivate returning and soft-delete missing customers
        load_snapshot_to_silver(
            spark,
            valid_df,
            silver_path,
            snapshot_customer_ids,
            "customer_id",
            [
                "first_name",
                "last_name",
                "email",
                "country",
                "registration_date",
                "is_active",
            ],
        )

        logger.info("Customers Silver and Quarantine data saved successfully")

    except Exception:
        # No SUCCESS on failure; retry will reuse the existing Bronze batch
        logger.exception("Customers Silver transformation failed")
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
