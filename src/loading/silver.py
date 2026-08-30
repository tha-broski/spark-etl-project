import logging
from delta.tables import DeltaTable
from pyspark.sql import functions as f

logger = logging.getLogger(__name__)


# used in main.py
def load_snapshot_to_silver(
    spark, source_df, silver_path, snapshot_ids, entity_id_column, compare_columns
):
    logger.info("Started loading silver")

    # First load -> Silver doesn't exist, write all valid rows
    if not DeltaTable.isDeltaTable(spark, silver_path):
        logger.info("Silver first load")
        source_df.write.format("delta").mode("append").save(silver_path)
        return

    # Not first load -> load Silver table as DeltaTable for MERGE
    target_table = DeltaTable.forPath(spark, silver_path)
    target_df = target_table.toDF()

    # Find entities from Silver that are missing in current snapshot (snapshot_ids comes from main.py and contains both valid and invalid entity IDs)
    missing_entities_df = target_df.join(
        snapshot_ids, on=entity_id_column, how="left_anti"
    )

    # Keep just the IDs for second merge to soft-delete missing entity
    missing_entity_ids = missing_entities_df.select(entity_id_column)
    logger.info("Started merging silver")

    # MERGE | matched + changed -> update | not matched -> insert new entity
    merge_condition = f"t.{entity_id_column} = s.{entity_id_column}"
    update_condition = " OR ".join(
        [f"NOT (t.{column} <=> s.{column})" for column in compare_columns]
    )
    (
        target_table.alias("t")
        .merge(
            source=source_df.alias("s"),
            condition=merge_condition,
        )
        .whenMatchedUpdateAll(condition=update_condition)
        .whenNotMatchedInsertAll()
    ).execute()
    logger.info("Merge completed")

    # Entities not in full snapshot -> mark as inactive (soft-deleted)
    (
        target_table.alias("t")
        .merge(
            source=missing_entity_ids.alias("m"),
            condition=f"t.{entity_id_column} = m.{entity_id_column}",
        )
        .whenMatchedUpdate(set={"is_active": f.lit(False)})
    ).execute()
    logger.info("Missing entities soft deleted")


def load_incremental_to_silver(
    spark, source_df, silver_path, entity_id_column, compare_columns
):
    logger.info("Started incremental silver load")

    # First load -> Silver doesn't exist, write all valid rows
    if not DeltaTable.isDeltaTable(spark, silver_path):
        logger.info("Silver first load")
        source_df.write.format("delta").mode("append").save(silver_path)
        return

    # Existing Silver -> upsert current incremental batch
    target_table = DeltaTable.forPath(spark, silver_path)

    merge_condition = f"t.{entity_id_column} = s.{entity_id_column}"

    # MERGE | matched + changed -> update | not matched -> insert new entity
    update_condition = " OR ".join(
        [f"NOT (t.{column} <=> s.{column})" for column in compare_columns]
    )

    target_table.alias("t").merge(
        source=source_df.alias("s"),
        condition=merge_condition,
    ).whenMatchedUpdateAll(
        condition=update_condition
    ).whenNotMatchedInsertAll().execute()
    logger.info("Merge completed")
