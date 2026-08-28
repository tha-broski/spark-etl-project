from delta.tables import DeltaTable
from pyspark.sql import functions as f


def mark_file_status(spark, source_file, file_hash, batch_id, status, control_path):
    processed_file = spark.range(1).select(
        f.lit(source_file).alias("source_file"),
        f.lit(file_hash).alias("file_hash"),
        f.lit(batch_id).alias("batch_id"),
        f.lit(status).alias("status"),
        f.current_timestamp().alias("processed_at"),
    )
    processed_file.write.format("delta").mode("append").save(control_path)


def get_file_state(spark, file_hash, control_path):
    if not DeltaTable.isDeltaTable(spark, control_path):
        return None
    df = (
        spark.read.format("delta")
        .load(control_path)
        .filter(f.col("file_hash") == file_hash)
    )
    return df.sort(f.desc("processed_at")).first()
