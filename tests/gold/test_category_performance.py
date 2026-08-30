from datetime import date
from decimal import Decimal

from gold.category_performance import build_category_performance


def test_build_category_performance(spark):
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
        (3, 2, 102, 3, Decimal("15.00")),
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
        (102, "Product C", "Category A"),
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

    result_df = build_category_performance(
        orders_df,
        order_items_df,
        products_df,
    )

    result = {row["category"]: row for row in result_df.collect()}

    assert len(result) == 2

    category_a = result["Category A"]

    assert category_a["order_count"] == 2
    assert category_a["items_sold"] == 5
    assert category_a["revenue"] == Decimal("65.00")
    assert category_a["average_selling_price"] == Decimal("13.00")

    category_b = result["Category B"]

    assert category_b["order_count"] == 1
    assert category_b["items_sold"] == 1
    assert category_b["revenue"] == Decimal("20.00")
    assert category_b["average_selling_price"] == Decimal("20.00")
