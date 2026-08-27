from delta.tables import DeltaTable
from pyspark.sql import functions as f


def load_products_to_silver(spark, source_df, silver_path, snapshot_product_ids):
    if not DeltaTable.isDeltaTable(spark, silver_path):
        source_df.write.format("delta").mode("append").save(silver_path)
        return
    target_table = DeltaTable.forPath(spark, silver_path)
    target_df = target_table.toDF()
    missing_products_df = target_df.join(
        snapshot_product_ids, on="product_id", how="left_anti"
    )
    missing_product_ids = missing_products_df.select("product_id")
    (
        target_table.alias("t")
        .merge(
            source=source_df.alias("s"),
            condition="t.product_id = s.product_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
    ).execute()
    (
        target_table.alias("t")
        .merge(
            source=missing_product_ids.alias("m"),
            condition="t.product_id = m.product_id",
        )
        .whenMatchedUpdate(set={"is_active": f.lit(False)})
    ).execute()
