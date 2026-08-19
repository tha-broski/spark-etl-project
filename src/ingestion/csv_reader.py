def read_csv(spark, path, schema):
    df = spark.read.option("header", "true").schema(schema).csv(path)
    return df
