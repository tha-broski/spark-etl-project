from decimal import Decimal

from transformations.order_items import transform_order_items


def test_transform_order_items(spark):
    data = [
        ("1", "10", "100", "2", "19.99"),
        ("2", "20", "200", "5", "100.50"),
    ]

    columns = [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
    ]

    bronze_df = spark.createDataFrame(
        data,
        columns,
    )

    result_df = transform_order_items(bronze_df)

    result = {row["order_item_id"]: row for row in result_df.collect()}

    item_1 = result[1]

    assert item_1["order_item_id"] == 1
    assert item_1["order_id"] == 10
    assert item_1["product_id"] == 100
    assert item_1["quantity"] == 2
    assert item_1["unit_price"] == Decimal("19.99")

    item_2 = result[2]

    assert item_2["order_item_id"] == 2
    assert item_2["order_id"] == 20
    assert item_2["product_id"] == 200
    assert item_2["quantity"] == 5
    assert item_2["unit_price"] == Decimal("100.50")
