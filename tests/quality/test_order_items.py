from decimal import Decimal

from quality.order_items import validate_order_items


def test_validate_order_items(spark):
    data = [
        (1, 10, 100, 2, Decimal("19.99")),
        (2, 20, 200, 1, Decimal("10.00")),
        (2, 30, 300, 1, Decimal("15.00")),
        (0, 40, 400, 1, Decimal("5.00")),
        (5, 0, 500, 1, Decimal("8.00")),
        (6, 60, 0, 1, Decimal("9.00")),
        (7, 70, 700, 0, Decimal("12.00")),
        (8, 80, 800, 2, Decimal("-1.00")),
        (9, 90, 900, None, Decimal("10.00")),
        (10, 100, 1000, 1, None),
    ]

    columns = [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
    ]

    df = spark.createDataFrame(
        data,
        columns,
    )

    result_df = validate_order_items(df)

    result = {
        (row["order_item_id"], row["order_id"]): row for row in result_df.collect()
    }

    assert result[(1, 10)]["is_valid"] is True

    assert result[(2, 20)]["is_valid"] is False
    assert "order_item_id is not unique" in result[(2, 20)]["quality_errors"]

    assert result[(2, 30)]["is_valid"] is False
    assert "order_item_id is not unique" in result[(2, 30)]["quality_errors"]

    assert result[(0, 40)]["is_valid"] is False
    assert "order_item_id <= 0" in result[(0, 40)]["quality_errors"]

    assert result[(5, 0)]["is_valid"] is False
    assert "order_id <= 0" in result[(5, 0)]["quality_errors"]

    assert result[(6, 60)]["is_valid"] is False
    assert "product_id <= 0" in result[(6, 60)]["quality_errors"]

    assert result[(7, 70)]["is_valid"] is False
    assert "quantity <= 0" in result[(7, 70)]["quality_errors"]

    assert result[(8, 80)]["is_valid"] is False
    assert "unit_price < 0" in result[(8, 80)]["quality_errors"]

    assert result[(9, 90)]["is_valid"] is False
    assert "quantity is NULL" in result[(9, 90)]["quality_errors"]

    assert result[(10, 100)]["is_valid"] is False
    assert "unit_price is NULL" in result[(10, 100)]["quality_errors"]
