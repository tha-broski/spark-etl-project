from datetime import date

from quality.orders import validate_orders


def test_validate_orders(spark):
    data = [
        (1, 10, date(2026, 1, 1), "completed"),
        (2, 20, date(2026, 1, 2), "pending"),
        (2, 30, date(2026, 1, 3), "completed"),
        (0, 40, date(2026, 1, 4), "completed"),
        (5, -1, date(2026, 1, 5), "completed"),
        (6, 60, None, "completed"),
        (7, 70, date(2026, 1, 7), ""),
        (8, 80, date(2026, 1, 8), "shipped"),
    ]

    columns = [
        "order_id",
        "customer_id",
        "order_date",
        "status",
    ]

    df = spark.createDataFrame(
        data,
        columns,
    )

    result_df = validate_orders(df)

    result = {(row["order_id"], row["customer_id"]): row for row in result_df.collect()}

    assert result[(1, 10)]["is_valid"] is True

    assert result[(2, 20)]["is_valid"] is False
    assert "order_id is not unique" in result[(2, 20)]["quality_errors"]

    assert result[(2, 30)]["is_valid"] is False
    assert "order_id is not unique" in result[(2, 30)]["quality_errors"]

    assert result[(0, 40)]["is_valid"] is False
    assert "order_id <= 0" in result[(0, 40)]["quality_errors"]

    assert result[(5, -1)]["is_valid"] is False
    assert "customer_id <= 0" in result[(5, -1)]["quality_errors"]

    assert result[(6, 60)]["is_valid"] is False
    assert "order_date is NULL" in result[(6, 60)]["quality_errors"]

    assert result[(7, 70)]["is_valid"] is False
    assert "status is empty" in result[(7, 70)]["quality_errors"]

    assert result[(8, 80)]["is_valid"] is False
    assert "status is invalid" in result[(8, 80)]["quality_errors"]
