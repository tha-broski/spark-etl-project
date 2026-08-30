from decimal import Decimal

from quality.products import validate_products


def test_validate_products(spark):
    data = [
        (1, "Laptop", "Electronics", Decimal("1999.99"), 10),
        (2, "Mouse", "Accessories", Decimal("99.50"), 25),
        (2, "Keyboard", "Accessories", Decimal("150.00"), 5),
        (0, "Monitor", "Electronics", Decimal("1200.00"), 4),
        (5, "", "Electronics", Decimal("500.00"), 3),
        (6, "Phone", "", Decimal("2500.00"), 8),
        (7, "Tablet", "Electronics", Decimal("-1.00"), 2),
        (8, "Cable", "Accessories", Decimal("20.00"), -5),
    ]

    columns = [
        "product_id",
        "name",
        "category",
        "price",
        "stock_quantity",
    ]

    df = spark.createDataFrame(
        data,
        columns,
    )

    result_df = validate_products(df)

    result = {(row["product_id"], row["name"]): row for row in result_df.collect()}

    assert result[(1, "Laptop")]["is_valid"] is True

    assert result[(2, "Mouse")]["is_valid"] is False
    assert "product_id is not unique" in result[(2, "Mouse")]["quality_errors"]

    assert result[(2, "Keyboard")]["is_valid"] is False
    assert "product_id is not unique" in result[(2, "Keyboard")]["quality_errors"]

    assert result[(0, "Monitor")]["is_valid"] is False
    assert "product_id <= 0" in result[(0, "Monitor")]["quality_errors"]

    assert result[(5, "")]["is_valid"] is False
    assert "name is empty" in result[(5, "")]["quality_errors"]

    assert result[(6, "Phone")]["is_valid"] is False
    assert "category is empty" in result[(6, "Phone")]["quality_errors"]

    assert result[(7, "Tablet")]["is_valid"] is False
    assert "price < 0" in result[(7, "Tablet")]["quality_errors"]

    assert result[(8, "Cable")]["is_valid"] is False
    assert "stock_quantity < 0" in result[(8, "Cable")]["quality_errors"]
