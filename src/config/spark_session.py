import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def create_spark_session():
    # Local Windows setup required by Spark/Hadoop
    os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

    # Create local Spark session with Delta Lake support
    builder = (
        SparkSession.builder.appName("Spark ETL Project")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    # Add Delta dependencies and start Spark
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Reduce Spark console noise
    spark.sparkContext.setLogLevel("ERROR")

    return spark
