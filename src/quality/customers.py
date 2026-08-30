from pyspark.sql import functions as f


def validate_customers(silver_df):
    # Find duplicates of customer_id in Silver-ready snapshot
    customer_counts = (
        silver_df.groupBy("customer_id").count().filter(f.col("count") > 1)
    )

    # Join duplicate info back to original to mark customers as unique/not unique
    silver_df_with_duplicates = silver_df.join(customer_counts, "customer_id", "left")
    silver_df_with_duplicates = silver_df_with_duplicates.withColumn(
        "is_unique", f.col("count").isNull()
    ).drop("count")

    # Build list of data quality errors for each row | used in main.py to split data into valid and invalid records
    silver_df_with_duplicates = silver_df_with_duplicates.withColumn(
        "quality_errors",
        f.array_compact(
            f.array(
                f.when(~f.col("is_unique"), "customer_id is not unique").otherwise(
                    f.lit(None)
                ),
                f.when(f.col("customer_id").isNull(), "customer_id is NULL").otherwise(
                    f.lit(None)
                ),
                f.when(f.col("customer_id") <= 0, "customer_id <= 0").otherwise(
                    f.lit(None)
                ),
                f.when(f.col("first_name") == "", "first_name is empty").otherwise(
                    f.lit(None)
                ),
                f.when(f.col("first_name").isNull(), "first_name is NULL").otherwise(
                    f.lit(None)
                ),
                f.when(f.col("last_name") == "", "last_name is empty").otherwise(
                    f.lit(None)
                ),
                f.when(f.col("last_name").isNull(), "last_name is NULL").otherwise(
                    f.lit(None)
                ),
                f.when(f.col("country") == "", "country is empty").otherwise(
                    f.lit(None)
                ),
                f.when(f.col("country").isNull(), "country is NULL").otherwise(
                    f.lit(None)
                ),
                f.when(f.col("email") == "", "email is empty").otherwise(f.lit(None)),
                f.when(f.col("email").isNull(), "email is NULL").otherwise(f.lit(None)),
                f.when(
                    ~f.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
                    "email format is invalid",
                ).otherwise(f.lit(None)),
                f.when(
                    f.col("registration_date").isNull(), "registration_date is NULL"
                ).otherwise(f.lit(None)),
            )
        ),
    )

    # No Quality errors -> valid row | quality error -> invalid row | main.py sends valid rows to Silver and invalid rows to Quarantine
    silver_df_with_duplicates = silver_df_with_duplicates.withColumn(
        "is_valid", f.size("quality_errors") == 0
    )
    return silver_df_with_duplicates
