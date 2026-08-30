from loading.silver import load_snapshot_to_silver


def test_snapshot_silver_update_insert_and_soft_delete(spark, tmp_path):
    silver_path = str(tmp_path / "silver")

    initial_data = [
        (1, "Laptop", "Electronics", 1000.0, 10, True),
        (2, "Mouse", "Accessories", 50.0, 20, True),
    ]

    columns = [
        "product_id",
        "name",
        "category",
        "price",
        "stock_quantity",
        "is_active",
    ]

    initial_df = spark.createDataFrame(
        initial_data,
        columns,
    )

    initial_df.write.format("delta").mode("overwrite").save(silver_path)

    source_data = [
        (1, "Laptop Pro", "Electronics", 1200.0, 8, True),
        (3, "Keyboard", "Accessories", 100.0, 15, True),
    ]

    source_df = spark.createDataFrame(
        source_data,
        columns,
    )

    snapshot_ids = source_df.select("product_id")

    load_snapshot_to_silver(
        spark,
        source_df,
        silver_path,
        snapshot_ids,
        "product_id",
        [
            "name",
            "category",
            "price",
            "stock_quantity",
            "is_active",
        ],
    )

    result_df = spark.read.format("delta").load(silver_path)

    result = {row["product_id"]: row for row in result_df.collect()}

    assert len(result) == 3

    assert result[1]["name"] == "Laptop Pro"
    assert result[1]["price"] == 1200.0
    assert result[1]["stock_quantity"] == 8
    assert result[1]["is_active"] is True

    assert result[2]["name"] == "Mouse"
    assert result[2]["is_active"] is False

    assert result[3]["name"] == "Keyboard"
    assert result[3]["is_active"] is True


def test_snapshot_silver_reactivates_returning_record(spark, tmp_path):
    silver_path = str(tmp_path / "silver")

    initial_data = [
        (1, "Laptop", "Electronics", 1000.0, 10, True),
        (2, "Mouse", "Accessories", 50.0, 20, False),
    ]

    columns = [
        "product_id",
        "name",
        "category",
        "price",
        "stock_quantity",
        "is_active",
    ]

    initial_df = spark.createDataFrame(
        initial_data,
        columns,
    )

    initial_df.write.format("delta").mode("overwrite").save(silver_path)

    source_data = [
        (1, "Laptop", "Electronics", 1000.0, 10, True),
        (2, "Mouse", "Accessories", 55.0, 25, True),
    ]

    source_df = spark.createDataFrame(
        source_data,
        columns,
    )

    snapshot_ids = source_df.select("product_id")

    load_snapshot_to_silver(
        spark,
        source_df,
        silver_path,
        snapshot_ids,
        "product_id",
        [
            "name",
            "category",
            "price",
            "stock_quantity",
            "is_active",
        ],
    )

    result_df = spark.read.format("delta").load(silver_path)

    result = {row["product_id"]: row for row in result_df.collect()}

    assert result[2]["price"] == 55.0
    assert result[2]["stock_quantity"] == 25
    assert result[2]["is_active"] is True
