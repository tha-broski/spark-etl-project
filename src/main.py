from config.spark_session import create_spark_session
from schemas.ecommerce import products_schema
from ingestion.csv_reader import read_csv
from pyspark.sql import functions as f
import uuid

spark = create_spark_session()
path = "data/raw/products.csv"
df = read_csv(spark, path, products_schema)
batch_id = str(uuid.uuid4())
df = df.withColumn("ingestion_timestamp", f.current_timestamp())
df = df.withColumn("source_file", f.input_file_name())
df = df.withColumn("batch_id", f.lit(batch_id))
df.write.format("delta").mode("append").save("data/bronze/products")
df.printSchema()
df.show()

spark.stop()
