from pyspark.sql import functions as f


def validate_order_items(silver_df):
    # Find duplicates of order_item_id in current batch
    order_item_counts = (
        silver_df.groupBy("order_item_id").count().filter(f.col("count") > 1)
    )

    # Join duplicate info back to original rows
    silver_df_with_duplicates = silver_df.join(
        order_item_counts,
        "order_item_id",
        "left",
    )

    silver_df_with_duplicates = silver_df_with_duplicates.withColumn(
        "is_unique",
        f.col("count").isNull(),
    ).drop("count")

    # Build list of data quality errors for each row
    silver_df_with_duplicates = silver_df_with_duplicates.withColumn(
        "quality_errors",
        f.array_compact(
            f.array(
                f.when(
                    ~f.col("is_unique"),
                    "order_item_id is not unique",
                ).otherwise(f.lit(None)),
                f.when(
                    f.col("order_item_id").isNull(),
                    "order_item_id is NULL",
                ).otherwise(f.lit(None)),
                f.when(
                    f.col("order_item_id") <= 0,
                    "order_item_id <= 0",
                ).otherwise(f.lit(None)),
                f.when(
                    f.col("order_id").isNull(),
                    "order_id is NULL",
                ).otherwise(f.lit(None)),
                f.when(
                    f.col("order_id") <= 0,
                    "order_id <= 0",
                ).otherwise(f.lit(None)),
                f.when(
                    f.col("product_id").isNull(),
                    "product_id is NULL",
                ).otherwise(f.lit(None)),
                f.when(
                    f.col("product_id") <= 0,
                    "product_id <= 0",
                ).otherwise(f.lit(None)),
                f.when(
                    f.col("quantity").isNull(),
                    "quantity is NULL",
                ).otherwise(f.lit(None)),
                f.when(
                    f.col("quantity") <= 0,
                    "quantity <= 0",
                ).otherwise(f.lit(None)),
                f.when(
                    f.col("unit_price").isNull(),
                    "unit_price is NULL",
                ).otherwise(f.lit(None)),
                f.when(
                    f.col("unit_price") < 0,
                    "unit_price < 0",
                ).otherwise(f.lit(None)),
            )
        ),
    )

    # No quality errors -> valid row
    silver_df_with_duplicates = silver_df_with_duplicates.withColumn(
        "is_valid",
        f.size("quality_errors") == 0,
    )

    return silver_df_with_duplicates
