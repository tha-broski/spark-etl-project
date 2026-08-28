import logging
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)


def load_products_to_quarantine(spark, invalid_df, quarantine_path):
    logger.info("Started loading quarantine")
    if not DeltaTable.isDeltaTable(spark, quarantine_path):
        logger.info("Quarantine first load")
        invalid_df.write.format("delta").mode("append").save(quarantine_path)
        return
    logger.info("Quarantine exists | Deduplicating current batch")
    existing_quarantine_df = spark.read.format("delta").load(quarantine_path)
    new_invalid_df = invalid_df.join(
        existing_quarantine_df,
        on=["batch_id", "product_id"],
        how="left_anti",
    )
    new_invalid_df.write.format("delta").mode("append").save(quarantine_path)
    logger.info("Quarantine saved")
