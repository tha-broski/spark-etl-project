import logging
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)


# used in main.py
def load_products_to_quarantine(spark, invalid_df, quarantine_path):
    logger.info("Started loading quarantine")

    # First load -> Quarantine non existent -> append all invalid rows
    if not DeltaTable.isDeltaTable(spark, quarantine_path):
        logger.info("Quarantine first load")
        invalid_df.write.format("delta").mode("append").save(quarantine_path)
        return

    # Quarantine already exists -> read the Quarantine rows
    logger.info("Quarantine exists | Deduplicating current batch")
    existing_quarantine_df = spark.read.format("delta").load(quarantine_path)

    # Compare old quarantine rows with new batch, remove duplicates, keep new invalid rows by batch_id + product_id
    new_invalid_df = invalid_df.join(
        existing_quarantine_df,
        on=["batch_id", "product_id"],
        how="left_anti",
    )

    # Append new invalid rows to Quarantine
    new_invalid_df.write.format("delta").mode("append").save(quarantine_path)
    logger.info("Quarantine saved")
