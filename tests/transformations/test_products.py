from decimal import Decimal

from transformations.products import transform_products


def test_transform_products(spark):
    data = [
        ("1", " Laptop ", " Electronics ", "1999.99", "10"),
        ("2", "Mouse", "Accessories", "99.50", "25"),
    ]

    columns = [
        "product_id",
        "name",
        "category",
        "price",
        "stock_quantity",
    ]

    bronze_df = spark.createDataFrame(
        data,
        columns,
    )

    result_df = transform_products(bronze_df)

    result = {row["product_id"]: row for row in result_df.collect()}

    product_1 = result[1]

    assert product_1["product_id"] == 1
    assert product_1["name"] == "Laptop"
    assert product_1["category"] == "Electronics"
    assert product_1["price"] == Decimal("1999.99")
    assert product_1["stock_quantity"] == 10
    assert product_1["is_active"] is True

    product_2 = result[2]

    assert product_2["product_id"] == 2
    assert product_2["name"] == "Mouse"
    assert product_2["category"] == "Accessories"
    assert product_2["price"] == Decimal("99.50")
    assert product_2["stock_quantity"] == 25
    assert product_2["is_active"] is True
