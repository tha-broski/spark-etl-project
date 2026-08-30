import logging

from delta.tables import DeltaTable
from pyspark.sql import functions as f
from ingestion.bronze import ingest_to_bronze
from utils.file_utils import calculate_file_hash

logger = logging.getLogger(__name__)


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


def prepare_file_batch(spark, source_path, schema, bronze_path, control_path):
    # utils/file_utils.py -> source file hashing logic used to identify the file content, used later to verify if the file has already been processed
    file_hash = calculate_file_hash(source_path)

    # Control state logic | verify if the file has been processed (New file/Bronze written/Success)
    file_state = get_file_state(spark, file_hash, control_path)

    # file processed -> skip the file | Prevents duplicates
    if file_state is not None and file_state.status == "SUCCESS":
        logger.info("File is already processed and will be skipped")
        return (file_hash, None, True)
    # Bronze written then fail -> reuse batch_id from control | prevents appending same stuff to the bronze twice
    elif file_state is not None and file_state.status == "BRONZE_WRITTEN":
        batch_id = file_state.batch_id
        return (file_hash, batch_id, False)

    # File hasn't been processed yet -> append it to Bronze | batch_id generated in ingestion/bronze.py
    elif file_state is None:
        batch_id = ingest_to_bronze(
            spark,
            source_path,
            schema,
            bronze_path,
        )

        # Update the status to "BRONZE_WRITTEN" in control/processed_files.py | allows to avoid duplicates if later stage fails
        mark_file_status(
            spark,
            source_path,
            file_hash,
            batch_id,
            "BRONZE_WRITTEN",
            control_path,
        )
        return (file_hash, batch_id, False)
