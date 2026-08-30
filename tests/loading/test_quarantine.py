from loading.quarantine import load_to_quarantine


def test_quarantine_is_idempotent(spark, tmp_path):
    quarantine_path = str(tmp_path / "quarantine")

    data = [
        (
            "batch-001",
            1,
            "invalid status",
        ),
    ]

    columns = [
        "batch_id",
        "order_id",
        "quality_errors",
    ]

    invalid_df = spark.createDataFrame(
        data,
        columns,
    )

    load_to_quarantine(
        spark,
        invalid_df,
        quarantine_path,
        "order_id",
    )

    load_to_quarantine(
        spark,
        invalid_df,
        quarantine_path,
        "order_id",
    )

    result_df = spark.read.format("delta").load(quarantine_path)

    result = result_df.collect()

    assert len(result) == 1
    assert result[0]["batch_id"] == "batch-001"
    assert result[0]["order_id"] == 1


def test_quarantine_keeps_same_entity_from_different_batches(spark, tmp_path):
    quarantine_path = str(tmp_path / "quarantine")

    first_batch = [
        (
            "batch-001",
            1,
            "invalid status",
        ),
    ]

    second_batch = [
        (
            "batch-002",
            1,
            "invalid status",
        ),
    ]

    columns = [
        "batch_id",
        "order_id",
        "quality_errors",
    ]

    first_df = spark.createDataFrame(
        first_batch,
        columns,
    )

    second_df = spark.createDataFrame(
        second_batch,
        columns,
    )

    load_to_quarantine(
        spark,
        first_df,
        quarantine_path,
        "order_id",
    )

    load_to_quarantine(
        spark,
        second_df,
        quarantine_path,
        "order_id",
    )

    result_df = spark.read.format("delta").load(quarantine_path)

    result = result_df.collect()

    assert len(result) == 2

    batch_ids = {row["batch_id"] for row in result}

    assert batch_ids == {
        "batch-001",
        "batch-002",
    }
