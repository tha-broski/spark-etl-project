from datetime import date
from decimal import Decimal

from gold.daily_sales import build_daily_sales


def test_build_daily_sales(spark):
    orders_data = [
        (1, 10, date(2026, 1, 1), "completed"),
        (2, 20, date(2026, 1, 1), "completed"),
        (3, 30, date(2026, 1, 1), "cancelled"),
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
        (3, 2, 100, 3, Decimal("10.00")),
        (4, 3, 102, 5, Decimal("100.00")),
    ]

    order_items_columns = [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
    ]

    orders_df = spark.createDataFrame(
        orders_data,
        orders_columns,
    )

    order_items_df = spark.createDataFrame(
        order_items_data,
        order_items_columns,
    )

    result_df = build_daily_sales(
        orders_df,
        order_items_df,
    )

    result = result_df.collect()

    assert len(result) == 1

    row = result[0]

    assert row["order_date"] == date(2026, 1, 1)
    assert row["order_count"] == 2
    assert row["items_sold"] == 6
    assert row["revenue"] == Decimal("70.00")
    assert row["average_order_value"] == Decimal("35.00")
