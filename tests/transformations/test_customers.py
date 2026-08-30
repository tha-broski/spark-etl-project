from datetime import date

from transformations.customers import transform_customers


def test_transform_customers(spark):
    data = [
        (
            "1",
            " John ",
            " Smith ",
            " john@example.com ",
            " Poland ",
            "2026-01-05",
        ),
        (
            "2",
            "Anna",
            "Brown",
            "anna@example.com",
            "Germany",
            "2026-02-10",
        ),
    ]

    columns = [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "country",
        "registration_date",
    ]

    bronze_df = spark.createDataFrame(
        data,
        columns,
    )

    result_df = transform_customers(bronze_df)

    result = {row["customer_id"]: row for row in result_df.collect()}

    customer_1 = result[1]

    assert customer_1["customer_id"] == 1
    assert customer_1["first_name"] == "John"
    assert customer_1["last_name"] == "Smith"
    assert customer_1["email"] == "john@example.com"
    assert customer_1["country"] == "Poland"
    assert customer_1["registration_date"] == date(2026, 1, 5)
    assert customer_1["is_active"] is True

    customer_2 = result[2]

    assert customer_2["customer_id"] == 2
    assert customer_2["first_name"] == "Anna"
    assert customer_2["last_name"] == "Brown"
    assert customer_2["email"] == "anna@example.com"
    assert customer_2["country"] == "Germany"
    assert customer_2["registration_date"] == date(2026, 2, 10)
    assert customer_2["is_active"] is True
