from delta.tables import DeltaTable
from pyspark.sql import functions as f


def mark_file_status(spark, source_file, file_hash, batch_id, status, control_path):
    # Create one row df with current pipeline state | called by main.py
    processed_file = spark.range(1).select(
        f.lit(source_file).alias("source_file"),
        f.lit(file_hash).alias("file_hash"),
        f.lit(batch_id).alias("batch_id"),
        f.lit(status).alias("status"),
        f.current_timestamp().alias("processed_at"),
    )

    # Append new state to Control history | the state will be used in get_file_state()
    processed_file.write.format("delta").mode("append").save(control_path)


def get_file_state(spark, file_hash, control_path):
    # No Control table -> file hasn't been processed
    if not DeltaTable.isDeltaTable(spark, control_path):
        return None

    # Find Control records for the current source file hash
    df = (
        spark.read.format("delta")
        .load(control_path)
        .filter(f.col("file_hash") == file_hash)
    )

    # Return latest found state (SUCCESS / BRONZE_WRITTEN) | called in main.py to decide what happens next
    return df.sort(f.desc("processed_at")).first()
