from pyspark.sql import functions as f


def validate_products(silver_df):
    # Find product_id duplicates in current Silver-ready snapshot
    product_counts = silver_df.groupBy("product_id").count().filter(f.col("count") > 1)

    # Join duplicate info back to original to mark products as unique/not unique
    silver_df_with_duplicates = silver_df.join(product_counts, "product_id", "left")
    silver_df_with_duplicates = silver_df_with_duplicates.withColumn(
        "is_unique", f.col("count").isNull()
    ).drop("count")

    # Build list of data quality errors for each row | used in main.py to split between valid and invalid records
    silver_df_with_duplicates = silver_df_with_duplicates.withColumn(
        "quality_errors",
        f.array_compact(
            f.array(
                f.when(~f.col("is_unique"), "product_id is not unique").otherwise(
                    f.lit(None)
                ),
                f.when(f.col("product_id").isNull(), "product_id is NULL").otherwise(
                    f.lit(None)
                ),
                f.when(f.col("product_id") <= 0, "product_id <= 0").otherwise(
                    f.lit(None)
                ),
                f.when(f.col("name") == "", "name is empty").otherwise(f.lit(None)),
                f.when(
                    f.col("name").isNull(),
                    "name is NULL",
                ).otherwise(f.lit(None)),
                f.when(f.col("category") == "", "category is empty").otherwise(
                    f.lit(None)
                ),
                f.when(
                    f.col("category").isNull(),
                    "category is NULL",
                ).otherwise(f.lit(None)),
                f.when(f.col("price") < 0, "price < 0").otherwise(f.lit(None)),
                f.when(
                    f.col("price").isNull(),
                    "price is NULL",
                ).otherwise(f.lit(None)),
                f.when(
                    f.col("stock_quantity") < 0,
                    "stock_quantity < 0",
                ).otherwise(f.lit(None)),
                f.when(
                    f.col("stock_quantity").isNull(),
                    "stock_quantity is NULL",
                ).otherwise(f.lit(None)),
            ),
        ),
    )

    # No quality errors -> valid row | quality error -> invalid row | main.py sends valid rows to Silver and invalid rows to Quarantine
    silver_df_with_duplicates = silver_df_with_duplicates.withColumn(
        "is_valid", f.size("quality_errors") == 0
    )
    return silver_df_with_duplicates
