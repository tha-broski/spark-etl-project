import logging

from config.spark_session import create_spark_session
from pipelines.products_pipeline import process_products
from pipelines.customers_pipeline import process_customers
from pipelines.orders_pipeline import process_orders
from pipelines.order_items_pipeline import process_order_items
from pipelines.gold_pipeline import build_gold_layer
from config import paths

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def main():
    # config/spark_session.py -> Spark and delta config
    spark = create_spark_session()

    logger.info("Pipeline started")
    try:
        process_products(
            spark,
            paths.PRODUCTS_SOURCE_PATH,
            paths.PRODUCTS_BRONZE_PATH,
            paths.PRODUCTS_SILVER_PATH,
            paths.PRODUCTS_QUARANTINE_PATH,
            paths.CONTROL_PATH,
        )

        process_customers(
            spark,
            paths.CUSTOMERS_SOURCE_PATH,
            paths.CUSTOMERS_BRONZE_PATH,
            paths.CUSTOMERS_SILVER_PATH,
            paths.CUSTOMERS_QUARANTINE_PATH,
            paths.CONTROL_PATH,
        )

        process_orders(
            spark,
            paths.ORDERS_SOURCE_PATH,
            paths.ORDERS_BRONZE_PATH,
            paths.ORDERS_SILVER_PATH,
            paths.ORDERS_QUARANTINE_PATH,
            paths.CONTROL_PATH,
        )

        process_order_items(
            spark,
            paths.ORDER_ITEMS_SOURCE_PATH,
            paths.ORDER_ITEMS_BRONZE_PATH,
            paths.ORDER_ITEMS_SILVER_PATH,
            paths.ORDER_ITEMS_QUARANTINE_PATH,
            paths.CONTROL_PATH,
        )

        build_gold_layer(
            spark,
            paths.ORDERS_SILVER_PATH,
            paths.ORDER_ITEMS_SILVER_PATH,
            paths.PRODUCTS_SILVER_PATH,
            paths.CUSTOMERS_SILVER_PATH,
            paths.DAILY_SALES_GOLD_PATH,
            paths.PRODUCT_PERFORMANCE_GOLD_PATH,
            paths.CATEGORY_PERFORMANCE_GOLD_PATH,
            paths.CUSTOMER_METRICS_GOLD_PATH,
        )

        logger.info("Pipeline completed successfully")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
