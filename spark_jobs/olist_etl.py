"""Production-style PySpark ETL job for the Olist Brazilian E-Commerce dataset.

Reads raw CSVs from HDFS (or local path), enforces explicit schemas, cleans nulls,
duplicates, bad timestamps, and price/freight outliers, builds star-schema fact
and dimension tables, derives analytics features, and writes partitioned Parquet.
"""

import argparse
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from schemas import SCHEMA_REGISTRY


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Olist PySpark ETL Pipeline")
    parser.add_argument(
        "--input-base",
        default="hdfs://localhost:9000/data/olist/raw",
        help="Base HDFS or local directory containing the 9 raw Olist CSVs.",
    )
    parser.add_argument(
        "--output-base",
        default="hdfs://localhost:9000/data/olist/processed",
        help="Base HDFS or local directory for output Parquet tables.",
    )
    return parser.parse_args()


def load_raw_tables(spark: SparkSession, input_base: str) -> Dict[str, DataFrame]:
    """Read all 9 Olist CSVs using explicit schemas."""
    tables: Dict[str, DataFrame] = {}
    base = input_base.rstrip("/")
    for name, (filename, schema) in SCHEMA_REGISTRY.items():
        path = f"{base}/{filename}"
        df = (
            spark.read.option("header", "true")
            .option("mode", "DROPMALFORMED")
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .schema(schema)
            .csv(path)
        )
        tables[name] = df
        print(f"[LOAD] {name:<22} raw_rows={df.count()}")
    return tables


def clean_orders(orders_df: DataFrame) -> DataFrame:
    """Clean orders: drop null keys/timestamps, deduplicate, fix invalid delivery dates."""
    cleaned = (
        orders_df.dropna(subset=["order_id", "customer_id", "order_purchase_timestamp"])
        .dropDuplicates(["order_id"])
        .filter(
            F.col("order_delivered_customer_date").isNull()
            | (F.col("order_delivered_customer_date") >= F.col("order_purchase_timestamp"))
        )
    )
    return cleaned


def clean_order_items(items_df: DataFrame) -> DataFrame:
    """Clean order items: drop null keys, deduplicate, and remove price/freight outliers."""
    base = items_df.dropna(subset=["order_id", "order_item_id", "product_id", "seller_id"]).dropDuplicates(
        ["order_id", "order_item_id"]
    )
    valid = base.filter((F.col("price") > 0) & (F.col("freight_value") >= 0))

    # IQR-based upper bound to remove extreme data-entry outliers while keeping legitimate high-ticket items
    price_q1, price_q3 = valid.approxQuantile("price", [0.25, 0.75], 0.01)
    price_upper = price_q3 + 3.0 * (price_q3 - price_q1)

    freight_q1, freight_q3 = valid.approxQuantile("freight_value", [0.25, 0.75], 0.01)
    freight_upper = freight_q3 + 3.0 * (freight_q3 - freight_q1)

    cleaned = valid.filter(
        (F.col("price") <= F.lit(price_upper)) & (F.col("freight_value") <= F.lit(freight_upper))
    )
    print(
        f"[CLEAN] order_items upper bounds: price<={price_upper:.2f}, "
        f"freight<={freight_upper:.2f}, kept={cleaned.count()} / {items_df.count()}"
    )
    return cleaned


def aggregate_payments(payments_df: DataFrame) -> DataFrame:
    """Aggregate payments to order grain to avoid fan-out on multi-payment orders."""
    return (
        payments_df.dropna(subset=["order_id"])
        .filter(F.col("payment_value") > 0)
        .groupBy("order_id")
        .agg(
            F.sum("payment_value").alias("order_total_payment"),
            F.max("payment_installments").alias("max_installments"),
            F.first("payment_type", ignorenulls=True).alias("primary_payment_type"),
        )
    )


def aggregate_reviews(reviews_df: DataFrame) -> DataFrame:
    """Aggregate reviews to order grain."""
    return (
        reviews_df.dropna(subset=["order_id", "review_id"])
        .dropDuplicates(["review_id"])
        .filter(F.col("review_score").between(1, 5))
        .groupBy("order_id")
        .agg(F.avg("review_score").alias("review_score"))
    )


def aggregate_geolocation(geo_df: DataFrame) -> DataFrame:
    """Deduplicate geolocation to 1 row per zip_code_prefix using mean coordinates."""
    return (
        geo_df.dropna(subset=["geolocation_zip_code_prefix"])
        .groupBy("geolocation_zip_code_prefix")
        .agg(
            F.avg("geolocation_lat").alias("lat"),
            F.avg("geolocation_lng").alias("lng"),
        )
    )


def build_dimensions(tables: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """Create star-schema dimension tables."""
    geo_agg = aggregate_geolocation(tables["geolocation"])

    dim_customers = (
        tables["customers"]
        .dropna(subset=["customer_id", "customer_unique_id"])
        .dropDuplicates(["customer_id"])
        .join(
            geo_agg,
            F.col("customer_zip_code_prefix") == F.col("geolocation_zip_code_prefix"),
            "left",
        )
        .select(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            F.initcap("customer_city").alias("customer_city"),
            F.upper("customer_state").alias("customer_state"),
            F.col("lat").alias("customer_lat"),
            F.col("lng").alias("customer_lng"),
        )
    )

    dim_products = (
        tables["products"]
        .dropna(subset=["product_id"])
        .dropDuplicates(["product_id"])
        .join(tables["category_translation"], on="product_category_name", how="left")
        .select(
            "product_id",
            F.coalesce("product_category_name_english", "product_category_name", F.lit("unknown")).alias(
                "product_category"
            ),
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        )
    )

    dim_sellers = (
        tables["sellers"]
        .dropna(subset=["seller_id"])
        .dropDuplicates(["seller_id"])
        .join(
            geo_agg,
            F.col("seller_zip_code_prefix") == F.col("geolocation_zip_code_prefix"),
            "left",
        )
        .select(
            "seller_id",
            "seller_zip_code_prefix",
            F.initcap("seller_city").alias("seller_city"),
            F.upper("seller_state").alias("seller_state"),
            F.col("lat").alias("seller_lat"),
            F.col("lng").alias("seller_lng"),
        )
    )

    return {
        "dim_customers": dim_customers,
        "dim_products": dim_products,
        "dim_sellers": dim_sellers,
    }


def build_fact_order_items(
    orders: DataFrame,
    items: DataFrame,
    payments: DataFrame,
    reviews: DataFrame,
    dims: Dict[str, DataFrame],
) -> DataFrame:
    """Join cleaned tables into the central fact table and derive analytics features."""
    fact = (
        items.join(orders, on="order_id", how="inner")
        .join(payments, on="order_id", how="left")
        .join(reviews, on="order_id", how="left")
        .join(
            dims["dim_customers"].select("customer_id", "customer_unique_id", "customer_state"),
            on="customer_id",
            how="left",
        )
        .join(
            dims["dim_products"].select("product_id", "product_category"),
            on="product_id",
            how="left",
        )
        .join(
            dims["dim_sellers"].select("seller_id", "seller_state"),
            on="seller_id",
            how="left",
        )
    )

    return (
        fact.withColumn(
            "delivery_days",
            F.when(
                F.col("order_delivered_customer_date").isNotNull(),
                F.datediff("order_delivered_customer_date", "order_purchase_timestamp"),
            ),
        )
        .withColumn(
            "is_late_delivery",
            F.when(
                F.col("order_delivered_customer_date").isNotNull()
                & F.col("order_estimated_delivery_date").isNotNull(),
                F.when(
                    F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"),
                    F.lit(1),
                ).otherwise(F.lit(0)),
            ),
        )
        .withColumn("order_value", F.round(F.col("price") + F.col("freight_value"), 2))
        .withColumn("order_year", F.year("order_purchase_timestamp"))
        .withColumn("order_month", F.month("order_purchase_timestamp"))
        .withColumn("order_weekday", F.date_format("order_purchase_timestamp", "E"))
        .select(
            "order_id",
            "order_item_id",
            "customer_id",
            "customer_unique_id",
            "product_id",
            "seller_id",
            "order_status",
            "order_purchase_timestamp",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "price",
            "freight_value",
            "order_value",
            "order_total_payment",
            "primary_payment_type",
            "max_installments",
            "review_score",
            "delivery_days",
            "is_late_delivery",
            "customer_state",
            "seller_state",
            "product_category",
            "order_weekday",
            "order_year",
            "order_month",
        )
    )


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("OlistEcommerceETL")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    tables = load_raw_tables(spark, args.input_base)
    orders_clean = clean_orders(tables["orders"])
    items_clean = clean_order_items(tables["order_items"])
    payments_agg = aggregate_payments(tables["order_payments"])
    reviews_agg = aggregate_reviews(tables["order_reviews"])
    dims = build_dimensions(tables)

    fact_df = build_fact_order_items(orders_clean, items_clean, payments_agg, reviews_agg, dims)
    fact_count = fact_df.count()
    print(f"[FACT] fact_order_items rows={fact_count}")

    out_base = args.output_base.rstrip("/")
    (
        fact_df.write.mode("overwrite")
        .partitionBy("order_year", "order_month")
        .parquet(f"{out_base}/fact_order_items")
    )
    for dim_name, dim_df in dims.items():
        dim_count = dim_df.count()
        print(f"[DIM]  {dim_name:<18} rows={dim_count}")
        dim_df.write.mode("overwrite").parquet(f"{out_base}/{dim_name}")

    print(f"ETL completed successfully. Output written to {out_base}")
    spark.stop()


if __name__ == "__main__":
    main()

