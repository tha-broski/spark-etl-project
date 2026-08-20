from ingestion.csv_reader import read_csv
from pyspark.sql import functions as f
import uuid
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def ingest_to_bronze(spark, source_path, schema, bronze_path):
    logger.info("Bronze ingestion started")
    try:
        df = read_csv(spark, source_path, schema)
        batch_id = str(uuid.uuid4())
        logger.info("batch_id=%s", batch_id)
        df = df.withColumn("ingestion_timestamp", f.current_timestamp())
        df = df.withColumn("source_file", f.input_file_name())
        df = df.withColumn("batch_id", f.lit(batch_id))
        logger.info(
            "Bronze Ingestion | source=%s | target=%s", source_path, bronze_path
        )
        df.write.format("delta").mode("append").save(bronze_path)
        logger.info("Bronze data saved successfully")
    except Exception:
        logger.exception("Bronze ingestion failed")
