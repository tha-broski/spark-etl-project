from datetime import date

from quality.customers import validate_customers


def test_validate_customers(spark):
    data = [
        (1, "John", "Smith", "john@example.com", "Poland", date(2026, 1, 1)),
        (2, "Anna", "Brown", "anna@example.com", "Germany", date(2026, 1, 2)),
        (2, "Mark", "White", "mark@example.com", "France", date(2026, 1, 3)),
        (0, "Kate", "Black", "kate@example.com", "Spain", date(2026, 1, 4)),
        (5, "", "Green", "green@example.com", "Italy", date(2026, 1, 5)),
        (6, "Mike", "", "mike@example.com", "Poland", date(2026, 1, 6)),
        (7, "Tom", "Blue", "", "Germany", date(2026, 1, 7)),
        (8, "Lucy", "Gray", "wrong-email", "France", date(2026, 1, 8)),
        (9, "Adam", "Red", "adam@example.com", "", date(2026, 1, 9)),
        (10, "Eva", "Gold", "eva@example.com", "Spain", None),
    ]

    columns = [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "country",
        "registration_date",
    ]

    df = spark.createDataFrame(
        data,
        columns,
    )

    result_df = validate_customers(df)

    result = {(row["customer_id"], row["email"]): row for row in result_df.collect()}

    assert result[(1, "john@example.com")]["is_valid"] is True

    assert result[(2, "anna@example.com")]["is_valid"] is False
    assert (
        "customer_id is not unique" in result[(2, "anna@example.com")]["quality_errors"]
    )

    assert result[(2, "mark@example.com")]["is_valid"] is False
    assert (
        "customer_id is not unique" in result[(2, "mark@example.com")]["quality_errors"]
    )

    assert result[(0, "kate@example.com")]["is_valid"] is False
    assert "customer_id <= 0" in result[(0, "kate@example.com")]["quality_errors"]

    assert result[(5, "green@example.com")]["is_valid"] is False
    assert "first_name is empty" in result[(5, "green@example.com")]["quality_errors"]

    assert result[(6, "mike@example.com")]["is_valid"] is False
    assert "last_name is empty" in result[(6, "mike@example.com")]["quality_errors"]

    assert result[(7, "")]["is_valid"] is False
    assert "email is empty" in result[(7, "")]["quality_errors"]

    assert result[(8, "wrong-email")]["is_valid"] is False
    assert "email format is invalid" in result[(8, "wrong-email")]["quality_errors"]

    assert result[(9, "adam@example.com")]["is_valid"] is False
    assert "country is empty" in result[(9, "adam@example.com")]["quality_errors"]

    assert result[(10, "eva@example.com")]["is_valid"] is False
    assert (
        "registration_date is NULL" in result[(10, "eva@example.com")]["quality_errors"]
    )
