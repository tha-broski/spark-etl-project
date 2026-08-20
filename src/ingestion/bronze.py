from ingestion.csv_reader import read_csv
from pyspark.sql import functions as f
import uuid


def ingest_to_bronze(spark, source_path, schema, bronze_path):
    df = read_csv(spark, source_path, schema)
    batch_id = str(uuid.uuid4())
    df = df.withColumn("ingestion_timestamp", f.current_timestamp())
    df = df.withColumn("source_file", f.input_file_name())
    df = df.withColumn("batch_id", f.lit(batch_id))
    df.write.format("delta").mode("append").save(bronze_path)
