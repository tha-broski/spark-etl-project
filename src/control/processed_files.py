from delta.tables import DeltaTable
from pyspark.sql import functions as f


def is_file_processed(spark, file_hash, control_path):
    if not DeltaTable.isDeltaTable(spark, control_path):
        return False
    df = (
        spark.read.format("delta")
        .load(control_path)
        .filter((f.col("file_hash") == file_hash) & (f.col("status") == "SUCCESS"))
    )
    return df.first() is not None


def mark_file_processed(spark, source_file, file_hash, batch_id, control_path):
    processed_file = spark.range(1).select(
        f.lit(source_file).alias("source_file"),
        f.lit(file_hash).alias("file_hash"),
        f.lit(batch_id).alias("batch_id"),
        f.current_timestamp().alias("processed_at"),
        f.lit("SUCCESS").alias("status"),
    )
    processed_file.write.format("delta").mode("append").save(control_path)
