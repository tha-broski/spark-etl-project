def read_csv(spark, path, schema):
    # Does it even need to be explained???
    df = spark.read.option("header", "true").schema(schema).csv(path)
    return df
