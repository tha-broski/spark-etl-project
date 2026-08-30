from decimal import Decimal

from loading.silver import load_incremental_to_silver


def test_incremental_silver_insert_update_and_keep_missing(spark, tmp_path):
    silver_path = str(tmp_path / "silver")
    initial_data = [
        (1, 10, "2026-01-01", "pending"),
        (2, 20, "2026-01-02", "completed"),
    ]

    columns = [
        "order_id",
        "customer_id",
        "order_date",
        "status",
    ]

    initial_df = spark.createDataFrame(
        initial_data,
        columns,
    )

    initial_df.write.format("delta").mode("overwrite").save(silver_path)

    source_data = [
        (1, 10, "2026-01-01", "completed"),
        (3, 30, "2026-01-03", "pending"),
    ]

    source_df = spark.createDataFrame(
        source_data,
        columns,
    )

    load_incremental_to_silver(
        spark,
        source_df,
        silver_path,
        "order_id",
        [
            "customer_id",
            "order_date",
            "status",
        ],
    )

    result_df = spark.read.format("delta").load(silver_path)

    result = {row["order_id"]: row for row in result_df.collect()}

    assert len(result) == 3

    assert result[1]["status"] == "completed"

    assert result[2]["status"] == "completed"

    assert result[3]["customer_id"] == 30
    assert result[3]["status"] == "pending"
