from datetime import date
from decimal import Decimal

from gold.product_performance import build_product_performance


def test_build_product_performance(spark):
    orders_data = [
        (1, 10, date(2026, 1, 1), "completed"),
        (2, 20, date(2026, 1, 2), "completed"),
        (3, 30, date(2026, 1, 3), "cancelled"),
    ]

    orders_columns = [
        "order_id",
        "customer_id",
        "order_date",
        "status",
    ]

    order_items_data = [
        (1, 1, 100, 2, Decimal("10.00")),
        (2, 1, 101, 1, Decimal("20.00")),
        (3, 2, 100, 3, Decimal("12.00")),
        (4, 3, 100, 5, Decimal("50.00")),
    ]

    order_items_columns = [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
    ]

    products_data = [
        (100, "Product A", "Category A"),
        (101, "Product B", "Category B"),
    ]

    products_columns = [
        "product_id",
        "name",
        "category",
    ]

    orders_df = spark.createDataFrame(
        orders_data,
        orders_columns,
    )

    order_items_df = spark.createDataFrame(
        order_items_data,
        order_items_columns,
    )

    products_df = spark.createDataFrame(
        products_data,
        products_columns,
    )

    result_df = build_product_performance(
        orders_df,
        order_items_df,
        products_df,
    )

    result = {row["product_id"]: row for row in result_df.collect()}

    assert len(result) == 2

    product_a = result[100]

    assert product_a["name"] == "Product A"
    assert product_a["category"] == "Category A"
    assert product_a["order_count"] == 2
    assert product_a["items_sold"] == 5
    assert product_a["revenue"] == Decimal("56.00")
    assert product_a["average_selling_price"] == Decimal("11.20")

    product_b = result[101]

    assert product_b["order_count"] == 1
    assert product_b["items_sold"] == 1
    assert product_b["revenue"] == Decimal("20.00")
    assert product_b["average_selling_price"] == Decimal("20.00")
