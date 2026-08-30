from datetime import date

from transformations.orders import transform_orders


def test_transform_orders(spark):
    data = [
        ("1", "10", "2026-01-05", " completed "),
        ("2", "20", "2026-01-06", "pending"),
    ]

    columns = [
        "order_id",
        "customer_id",
        "order_date",
        "status",
    ]

    bronze_df = spark.createDataFrame(
        data,
        columns,
    )

    result_df = transform_orders(bronze_df)

    result = {row["order_id"]: row for row in result_df.collect()}

    order_1 = result[1]

    assert order_1["order_id"] == 1
    assert order_1["customer_id"] == 10
    assert order_1["order_date"] == date(2026, 1, 5)
    assert order_1["status"] == "completed"

    order_2 = result[2]

    assert order_2["order_id"] == 2
    assert order_2["customer_id"] == 20
    assert order_2["order_date"] == date(2026, 1, 6)
    assert order_2["status"] == "pending"
