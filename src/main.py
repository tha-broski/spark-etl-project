import logging

from config.spark_session import create_spark_session
from pyspark.sql import functions as f
from transformations.products import transform_products
from transformations.customers import transform_customers
from transformations.order_items import transform_order_items
from transformations.orders import transform_orders
from schemas.ecommerce import (
    products_schema,
    customers_schema,
    orders_schema,
    order_items_schema,
)
from quality.products import validate_products
from quality.customers import validate_customers
from quality.order_items import validate_order_items
from quality.orders import validate_orders
from control.processed_files import mark_file_status, prepare_file_batch
from loading.silver import load_snapshot_to_silver, load_incremental_to_silver
from loading.quarantine import load_to_quarantine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

PRODUCTS_SOURCE_PATH = "data/raw/products.csv"
PRODUCTS_BRONZE_PATH = "data/bronze/products"
PRODUCTS_SILVER_PATH = "data/silver/products"
PRODUCTS_QUARANTINE_PATH = "data/quarantine/products"
CUSTOMERS_SOURCE_PATH = "data/raw/customers.csv"
CUSTOMERS_BRONZE_PATH = "data/bronze/customers"
CUSTOMERS_SILVER_PATH = "data/silver/customers"
CUSTOMERS_QUARANTINE_PATH = "data/quarantine/customers"
ORDERS_SOURCE_PATH = "data/raw/orders.csv"
ORDERS_BRONZE_PATH = "data/bronze/orders"
ORDERS_SILVER_PATH = "data/silver/orders"
ORDERS_QUARANTINE_PATH = "data/quarantine/orders"
ORDER_ITEMS_SOURCE_PATH = "data/raw/order_items.csv"
ORDER_ITEMS_BRONZE_PATH = "data/bronze/order_items"
ORDER_ITEMS_SILVER_PATH = "data/silver/order_items"
ORDER_ITEMS_QUARANTINE_PATH = "data/quarantine/order_items"
CONTROL_PATH = "data/control/processed_files"

logger = logging.getLogger(__name__)


def main():
    # config/spark_session.py -> Spark and delta config
    spark = create_spark_session()

    logger.info("Pipeline started")

    products_file_hash, products_batch_id, products_should_skip = prepare_file_batch(
        spark, PRODUCTS_SOURCE_PATH, products_schema, PRODUCTS_BRONZE_PATH, CONTROL_PATH
    )

    # Read Bronze and keep only current batch_id because Bronze can contain historical snapshots
    if not products_should_skip:
        bronze_df = spark.read.format("delta").load(PRODUCTS_BRONZE_PATH)
        current_batch_df = bronze_df.filter(f.col("batch_id") == products_batch_id)

        logger.info("Products Silver transformation started")

        # transformations/products.py -> transforming/casting data from bronze to silver
        silver_df = transform_products(current_batch_df)

        # quality/products.py -> Validate and check data quality before loading to silver
        # cache() used because validated_df is reused below
        validated_df = validate_products(silver_df).cache()

        # store the product IDs from current snapshot (including invalid rows) | used in loading/silver.py to detect missing products
        snapshot_product_ids = validated_df.select("product_id")

        # counting total/valid/invalid rows for logging and observability
        total_rows = validated_df.count()
        valid_rows = validated_df.filter(f.col("is_valid")).count()
        invalid_rows = validated_df.filter(~f.col("is_valid")).count()

        logger.info(
            "Products validation completed | total rows: %s | valid rows: %s | invalid rows: %s",
            total_rows,
            valid_rows,
            invalid_rows,
        )

        # after validation and quality check split the data into valid->Silver and invalid->Quarantine
        valid_df = validated_df.filter(f.col("is_valid"))
        invalid_df = validated_df.filter(~f.col("is_valid"))

        try:
            # loading/quarantine.py -> idempotent writing (by batch_id + product_id) of invalid records to Quarantine
            load_to_quarantine(
                spark, invalid_df, PRODUCTS_QUARANTINE_PATH, "product_id"
            )

            # loading/silver.py | New Product -> Insert | Changed Product -> Update | Inactive Product -> Reactivate (if needed) | Missing Product -> soft-delete
            load_snapshot_to_silver(
                spark,
                valid_df,
                PRODUCTS_SILVER_PATH,
                snapshot_product_ids,
                "product_id",
                [
                    "name",
                    "category",
                    "price",
                    "stock_quantity",
                    "is_active",
                ],
            )
            logger.info("Silver and Quarantine data saved successfully")
        except Exception:
            # Silver or Quarantine write failed -> no success | control stays at BRONZE_WRITTEN | retry will start from existing bronze batch
            logger.exception("Silver transformation failed")
            raise
        finally:
            # Remove the cached DataFrame
            validated_df.unpersist()

        # Everything went well -> write state as "SUCCESS" to control/processed_files.py | future runs for same hash will be skipped
        mark_file_status(
            spark,
            PRODUCTS_SOURCE_PATH,
            products_file_hash,
            products_batch_id,
            "SUCCESS",
            CONTROL_PATH,
        )

    customers_file_hash, customers_batch_id, customers_should_skip = prepare_file_batch(
        spark,
        CUSTOMERS_SOURCE_PATH,
        customers_schema,
        CUSTOMERS_BRONZE_PATH,
        CONTROL_PATH,
    )

    if not customers_should_skip:
        bronze_df = spark.read.format("delta").load(CUSTOMERS_BRONZE_PATH)
        current_batch_df = bronze_df.filter(f.col("batch_id") == customers_batch_id)

        logger.info("Customers Silver transformation started")

        # transformations/customers.py -> transforming/casting data from bronze to silver
        silver_df = transform_customers(current_batch_df)

        # quality/customers.py -> Validate and check data quality before loading to silver
        # cache() used because validated_df is reused below
        validated_df = validate_customers(silver_df).cache()

        # store the customer IDs from current snapshot (including invalid rows) | used in loading/silver.py to detect missing customers
        snapshot_customer_ids = validated_df.select("customer_id")

        # counting total/valid/invalid rows for logging and observability
        total_rows = validated_df.count()
        valid_rows = validated_df.filter(f.col("is_valid")).count()
        invalid_rows = validated_df.filter(~f.col("is_valid")).count()

        logger.info(
            "Customers validation completed | total rows: %s | valid rows: %s | invalid rows: %s",
            total_rows,
            valid_rows,
            invalid_rows,
        )

        # after validation and quality check split the data into valid->Silver and invalid->Quarantine
        valid_df = validated_df.filter(f.col("is_valid"))
        invalid_df = validated_df.filter(~f.col("is_valid"))

        try:
            # loading/quarantine.py -> idempotent writing (by batch_id + customer_id) of invalid records to Quarantine
            load_to_quarantine(
                spark, invalid_df, CUSTOMERS_QUARANTINE_PATH, "customer_id"
            )

            # loading/silver.py | New Customer -> Insert | Changed Customer -> Update | Inactive Customer -> Reactivate (if needed) | Missing Customer -> soft-delete
            load_snapshot_to_silver(
                spark,
                valid_df,
                CUSTOMERS_SILVER_PATH,
                snapshot_customer_ids,
                "customer_id",
                [
                    "first_name",
                    "last_name",
                    "email",
                    "country",
                    "registration_date",
                    "is_active",
                ],
            )
            logger.info("Silver and Quarantine data saved successfully")
        except Exception:
            # Silver or Quarantine write failed -> no success | control stays at BRONZE_WRITTEN | retry will start from existing bronze batch
            logger.exception("Silver transformation failed")
            raise
        finally:
            # Remove the cached DataFrame
            validated_df.unpersist()

        # Everything went well -> write state as "SUCCESS" to control/processed_files.py | future runs for same hash will be skipped
        mark_file_status(
            spark,
            CUSTOMERS_SOURCE_PATH,
            customers_file_hash,
            customers_batch_id,
            "SUCCESS",
            CONTROL_PATH,
        )

    orders_file_hash, orders_batch_id, orders_should_skip = prepare_file_batch(
        spark,
        ORDERS_SOURCE_PATH,
        orders_schema,
        ORDERS_BRONZE_PATH,
        CONTROL_PATH,
    )

    if not orders_should_skip:
        bronze_df = spark.read.format("delta").load(ORDERS_BRONZE_PATH)
        current_batch_df = bronze_df.filter(f.col("batch_id") == orders_batch_id)

        logger.info("Orders Silver transformation started")

        # transformations/orders.py -> transforming/casting Bronze data for Silver
        silver_df = transform_orders(current_batch_df)

        # quality/orders.py -> validate Orders before Silver load
        validated_df = validate_orders(silver_df).cache()

        # Count rows for logging and observability
        total_rows = validated_df.count()
        valid_rows = validated_df.filter(f.col("is_valid")).count()
        invalid_rows = validated_df.filter(~f.col("is_valid")).count()

        logger.info(
            "Orders validation completed | total rows: %s | valid rows: %s | invalid rows: %s",
            total_rows,
            valid_rows,
            invalid_rows,
        )

        # Split valid -> Silver and invalid -> Quarantine
        valid_df = validated_df.filter(f.col("is_valid"))
        invalid_df = validated_df.filter(~f.col("is_valid"))

        try:
            # Invalid records are deduplicated by batch_id + order_id
            load_to_quarantine(
                spark,
                invalid_df,
                ORDERS_QUARANTINE_PATH,
                "order_id",
            )

            # Incremental upsert | new Order -> insert | changed Order -> update
            # Orders missing from current batch remain unchanged in Silver
            load_incremental_to_silver(
                spark,
                valid_df,
                ORDERS_SILVER_PATH,
                "order_id",
                [
                    "customer_id",
                    "order_date",
                    "status",
                ],
            )

            logger.info("Orders Silver and Quarantine data saved successfully")

        except Exception:
            # No SUCCESS on failure -> retry will reuse existing Bronze batch
            logger.exception("Orders Silver transformation failed")
            raise

        finally:
            validated_df.unpersist()

        # Orders batch completed successfully
        mark_file_status(
            spark,
            ORDERS_SOURCE_PATH,
            orders_file_hash,
            orders_batch_id,
            "SUCCESS",
            CONTROL_PATH,
        )

    order_items_file_hash, order_items_batch_id, order_items_should_skip = (
        prepare_file_batch(
            spark,
            ORDER_ITEMS_SOURCE_PATH,
            order_items_schema,
            ORDER_ITEMS_BRONZE_PATH,
            CONTROL_PATH,
        )
    )

    if not order_items_should_skip:
        bronze_df = spark.read.format("delta").load(ORDER_ITEMS_BRONZE_PATH)
        current_batch_df = bronze_df.filter(f.col("batch_id") == order_items_batch_id)

        logger.info("Order Items Silver transformation started")

        # transformations/order_items.py -> casting Bronze data for Silver
        silver_df = transform_order_items(current_batch_df)

        # quality/order_items.py -> validate Order Items before Silver load
        validated_df = validate_order_items(silver_df).cache()

        total_rows = validated_df.count()
        valid_rows = validated_df.filter(f.col("is_valid")).count()
        invalid_rows = validated_df.filter(~f.col("is_valid")).count()

        logger.info(
            "Order Items validation completed | total rows: %s | valid rows: %s | invalid rows: %s",
            total_rows,
            valid_rows,
            invalid_rows,
        )

        # valid -> Silver | invalid -> Quarantine
        valid_df = validated_df.filter(f.col("is_valid"))
        invalid_df = validated_df.filter(~f.col("is_valid"))

        try:
            load_to_quarantine(
                spark,
                invalid_df,
                ORDER_ITEMS_QUARANTINE_PATH,
                "order_item_id",
            )

            # Incremental upsert | new item -> insert | changed item -> update
            # Missing items in current batch remain unchanged
            load_incremental_to_silver(
                spark,
                valid_df,
                ORDER_ITEMS_SILVER_PATH,
                "order_item_id",
                [
                    "order_id",
                    "product_id",
                    "quantity",
                    "unit_price",
                ],
            )

            logger.info("Order Items Silver and Quarantine data saved successfully")

        except Exception:
            logger.exception("Order Items Silver transformation failed")
            raise

        finally:
            validated_df.unpersist()

        mark_file_status(
            spark,
            ORDER_ITEMS_SOURCE_PATH,
            order_items_file_hash,
            order_items_batch_id,
            "SUCCESS",
            CONTROL_PATH,
        )

    logger.info("Pipeline completed successfully")

    spark.stop()


if __name__ == "__main__":
    main()
