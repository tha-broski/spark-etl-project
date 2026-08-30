from datetime import date
from decimal import Decimal

from gold.customer_metrics import build_customer_metrics


def test_build_customer_metrics(spark):
    orders_data = [
        (1, 10, date(2026, 1, 1), "completed"),
        (2, 10, date(2026, 1, 5), "completed"),
        (3, 20, date(2026, 1, 3), "completed"),
        (4, 10, date(2026, 1, 10), "cancelled"),
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
        (4, 3, 102, 1, Decimal("50.00")),
        (5, 4, 103, 10, Decimal("100.00")),
    ]

    order_items_columns = [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
    ]

    customers_data = [
        (10, "John", "Smith", "Poland"),
        (20, "Anna", "Brown", "Germany"),
    ]

    customers_columns = [
        "customer_id",
        "first_name",
        "last_name",
        "country",
    ]

    orders_df = spark.createDataFrame(
        orders_data,
        orders_columns,
    )

    order_items_df = spark.createDataFrame(
        order_items_data,
        order_items_columns,
    )

    customers_df = spark.createDataFrame(
        customers_data,
        customers_columns,
    )

    result_df = build_customer_metrics(
        orders_df,
        order_items_df,
        customers_df,
    )

    result = {row["customer_id"]: row for row in result_df.collect()}

    assert len(result) == 2

    customer_10 = result[10]

    assert customer_10["first_name"] == "John"
    assert customer_10["last_name"] == "Smith"
    assert customer_10["country"] == "Poland"
    assert customer_10["order_count"] == 2
    assert customer_10["items_bought"] == 6
    assert customer_10["total_spent"] == Decimal("70.00")
    assert customer_10["average_order_value"] == Decimal("35.00")
    assert customer_10["first_order_date"] == date(2026, 1, 1)
    assert customer_10["last_order_date"] == date(2026, 1, 5)

    customer_20 = result[20]

    assert customer_20["order_count"] == 1
    assert customer_20["items_bought"] == 1
    assert customer_20["total_spent"] == Decimal("50.00")
    assert customer_20["average_order_value"] == Decimal("50.00")
    assert customer_20["first_order_date"] == date(2026, 1, 3)
    assert customer_20["last_order_date"] == date(2026, 1, 3)
