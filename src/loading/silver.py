import logging
from delta.tables import DeltaTable
from pyspark.sql import functions as f

logger = logging.getLogger(__name__)


# used in main.py
def load_products_to_silver(spark, source_df, silver_path, snapshot_product_ids):
    logger.info("Started loading silver")

    # First load -> Silver doesn't exist, write all valid rows
    if not DeltaTable.isDeltaTable(spark, silver_path):
        logger.info("Silver first load")
        source_df.write.format("delta").mode("append").save(silver_path)
        return

    # Not first load -> load Silver table as DeltaTable for MERGE
    target_table = DeltaTable.forPath(spark, silver_path)
    target_df = target_table.toDF()

    # Find products from Silver that are missing in current snapshot (snapshot_product_ids comes from main.py and contains both valid and invalid product IDs)
    missing_products_df = target_df.join(
        snapshot_product_ids, on="product_id", how="left_anti"
    )

    # Keep just the IDs for second merge to soft-delete missing products
    missing_product_ids = missing_products_df.select("product_id")
    logger.info("Started merging silver")

    # MERGE | matched + changed -> update | not matched -> insert new product
    (
        target_table.alias("t")
        .merge(
            source=source_df.alias("s"),
            condition="t.product_id = s.product_id",
        )
        .whenMatchedUpdateAll(
            condition=(
                "NOT (t.name<=>s.name) OR NOT (t.category<=>s.category) OR NOT (t.price<=>s.price) OR NOT (t.stock_quantity<=>s.stock_quantity) OR NOT (t.is_active<=>s.is_active)"
            )
        )
        .whenNotMatchedInsertAll()
    ).execute()
    logger.info("Merge completed")

    # Products not in full snapshot -> mark as inactive (soft-deleted)
    (
        target_table.alias("t")
        .merge(
            source=missing_product_ids.alias("m"),
            condition="t.product_id = m.product_id",
        )
        .whenMatchedUpdate(set={"is_active": f.lit(False)})
    ).execute()
    logger.info("Missing products soft deleted")
