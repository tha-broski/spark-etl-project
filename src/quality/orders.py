from pyspark.sql import functions as f


def validate_orders(silver_df):
    # Find duplicates of order_id in Silver-ready snapshot
    order_counts = silver_df.groupBy("order_id").count().filter(f.col("count") > 1)

    # Join duplicate info back to original to mark customers as unique/not unique
    silver_df_with_duplicates = silver_df.join(order_counts, "order_id", "left")
    silver_df_with_duplicates = silver_df_with_duplicates.withColumn(
        "is_unique", f.col("count").isNull()
    ).drop("count")

    # Build list of data quality errors for each row | used in main.py to split data into valid and invalid records
    silver_df_with_duplicates = silver_df_with_duplicates.withColumn(
        "quality_errors",
        f.array_compact(
            f.array(
                f.when(~f.col("is_unique"), "order_id is not unique").otherwise(
                    f.lit(None)
                ),
                f.when(f.col("order_id").isNull(), "order_id is NULL").otherwise(
                    f.lit(None)
                ),
                f.when(f.col("order_id") <= 0, "order_id <= 0").otherwise(f.lit(None)),
                f.when(f.col("customer_id").isNull(), "customer_id is NULL").otherwise(
                    f.lit(None)
                ),
                f.when(f.col("customer_id") <= 0, "customer_id <= 0").otherwise(
                    f.lit(None)
                ),
                f.when(f.col("order_date").isNull(), "order_date is NULL").otherwise(
                    f.lit(None)
                ),
                f.when(f.col("status") == "", "status is empty").otherwise(f.lit(None)),
                f.when(f.col("status").isNull(), "status is NULL").otherwise(
                    f.lit(None)
                ),
                f.when(
                    ~f.col("status").isin("pending", "completed", "cancelled"),
                    "status is invalid",
                ).otherwise(f.lit(None)),
            )
        ),
    )

    # No Quality errors -> valid row | quality error -> invalid row | main.py sends valid rows to Silver and invalid rows to Quarantine
    silver_df_with_duplicates = silver_df_with_duplicates.withColumn(
        "is_valid", f.size("quality_errors") == 0
    )
    return silver_df_with_duplicates
