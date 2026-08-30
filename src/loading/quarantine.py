import logging
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)


# used in main.py
def load_to_quarantine(spark, invalid_df, quarantine_path, entity_id_column):
    logger.info("Started loading quarantine")

    # First load -> Quarantine non existent -> append all invalid rows
    if not DeltaTable.isDeltaTable(spark, quarantine_path):
        logger.info("Quarantine first load")
        invalid_df.write.format("delta").mode("append").save(quarantine_path)
        return

    # Quarantine already exists -> read the Quarantine rows
    logger.info("Quarantine exists | Deduplicating current batch")
    existing_quarantine_df = spark.read.format("delta").load(quarantine_path)

    # Keep only new invalid rows by batch_id + entity ID
    new_invalid_df = invalid_df.join(
        existing_quarantine_df,
        on=["batch_id", entity_id_column],
        how="left_anti",
    )

    # Append new invalid rows to Quarantine
    new_invalid_df.write.format("delta").mode("append").save(quarantine_path)
    logger.info("Quarantine saved")
