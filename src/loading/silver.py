import logging
from delta.tables import DeltaTable
from pyspark.sql import functions as f

logger = logging.getLogger(__name__)


def load_products_to_silver(spark, source_df, silver_path, snapshot_product_ids):
    logger.info("Started loading silver")
    if not DeltaTable.isDeltaTable(spark, silver_path):
        logger.info("Silver first load")
        source_df.write.format("delta").mode("append").save(silver_path)
        return
    target_table = DeltaTable.forPath(spark, silver_path)
    target_df = target_table.toDF()
    missing_products_df = target_df.join(
        snapshot_product_ids, on="product_id", how="left_anti"
    )
    missing_product_ids = missing_products_df.select("product_id")
    logger.info("Started merging silver")
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
    (
        target_table.alias("t")
        .merge(
            source=missing_product_ids.alias("m"),
            condition="t.product_id = m.product_id",
        )
        .whenMatchedUpdate(set={"is_active": f.lit(False)})
    ).execute()
    logger.info("Missing products soft deleted")
