"""Export aggregated Parquet fact metrics to clean CSVs for Power BI / Tableau."""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Olist Aggregated CSVs for BI")
    parser.add_argument(
        "--fact-path",
        default="hdfs://localhost:9000/data/olist/processed/fact_order_items",
        help="Path to fact_order_items Parquet (HDFS or local).",
    )
    parser.add_argument(
        "--output-dir",
        default="dashboard",
        help="Local directory to write CSV files for Power BI.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    spark = SparkSession.builder.appName("ExportOlistDashboardCSVs").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    delivered = spark.read.parquet(args.fact_path).filter(F.col("order_status") == "delivered").cache()

    exports = {
        "kpi_summary.csv": delivered.agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.count("*").alias("total_items"),
            F.round(F.sum("order_value"), 2).alias("total_revenue"),
            F.round(F.sum("order_value") / F.countDistinct("order_id"), 2).alias("avg_order_value"),
            F.round(F.avg("review_score"), 2).alias("avg_review_score"),
            F.round(100.0 * F.avg(F.col("is_late_delivery").cast("double")), 2).alias("late_delivery_pct"),
        ),
        "monthly_revenue.csv": delivered.groupBy("order_year", "order_month")
        .agg(
            F.countDistinct("order_id").alias("orders_count"),
            F.round(F.sum("order_value"), 2).alias("revenue"),
            F.round(F.avg("review_score"), 2).alias("avg_review_score"),
        )
        .orderBy("order_year", "order_month"),
        "category_revenue.csv": delivered.groupBy("product_category")
        .agg(
            F.countDistinct("order_id").alias("orders_count"),
            F.round(F.sum("order_value"), 2).alias("revenue"),
            F.round(F.avg("review_score"), 2).alias("avg_review_score"),
        )
        .orderBy(F.desc("revenue")),
        "state_delivery_metrics.csv": delivered.filter(F.col("delivery_days").isNotNull())
        .groupBy("customer_state")
        .agg(
            F.countDistinct("order_id").alias("orders_count"),
            F.round(F.sum("order_value"), 2).alias("revenue"),
            F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
            F.round(100.0 * F.avg(F.col("is_late_delivery").cast("double")), 2).alias("late_delivery_pct"),
            F.round(F.avg("review_score"), 2).alias("avg_review_score"),
        )
        .orderBy(F.desc("revenue")),
        "delay_vs_review.csv": delivered.filter(
            F.col("order_delivered_customer_date").isNotNull()
            & F.col("order_estimated_delivery_date").isNotNull()
            & F.col("review_score").isNotNull()
        )
        .withColumn(
            "delay_days",
            F.datediff("order_delivered_customer_date", "order_estimated_delivery_date"),
        )
        .withColumn(
            "delivery_bucket",
            F.when(F.col("delay_days") <= 0, "On-time / Early")
            .when(F.col("delay_days").between(1, 3), "1-3 Days Late")
            .when(F.col("delay_days").between(4, 7), "4-7 Days Late")
            .otherwise("8+ Days Late"),
        )
        .groupBy("delivery_bucket")
        .agg(
            F.countDistinct("order_id").alias("orders_count"),
            F.round(F.avg("review_score"), 2).alias("avg_review_score"),
        ),
        "payment_mix.csv": delivered.withColumn(
            "payment_type", F.coalesce("primary_payment_type", F.lit("unknown"))
        )
        .groupBy("payment_type")
        .agg(
            F.countDistinct("order_id").alias("orders_count"),
            F.round(F.sum("order_value"), 2).alias("revenue"),
            F.round(F.avg("max_installments"), 2).alias("avg_installments"),
        )
        .orderBy(F.desc("revenue")),
    }

    for filename, df in exports.items():
        out_path = os.path.join(args.output_dir, filename)
        df.toPandas().to_csv(out_path, index=False)
        print(f"[EXPORT] {out_path}")

    spark.stop()


if __name__ == "__main__":
    main()

