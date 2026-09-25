"""Benchmark the same multi-dimensional aggregation in Pandas vs. PySpark
on 1x (full Olist dataset) and 10x (duplicated) data.
"""

import argparse
import os
import time

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pandas vs Spark Aggregation Benchmark")
    parser.add_argument(
        "--fact-path",
        default="hdfs://localhost:9000/data/olist/processed/fact_order_items",
        help="Path to fact_order_items Parquet (HDFS or local).",
    )
    parser.add_argument(
        "--output-dir",
        default="docs",
        help="Directory to save benchmark_results.csv and benchmark_results.md",
    )
    return parser.parse_args()


def run_pandas_agg(df: pd.DataFrame) -> pd.DataFrame:
    delivered = df[df["order_status"] == "delivered"]
    return (
        delivered.groupby(
            ["order_year", "order_month", "customer_state", "product_category"],
            as_index=False,
            dropna=False,
        )
        .agg(
            total_revenue=("order_value", "sum"),
            order_count=("order_id", "nunique"),
            avg_delivery_days=("delivery_days", "mean"),
            late_rate=("is_late_delivery", "mean"),
            avg_review=("review_score", "mean"),
        )
    )


def run_spark_agg(sdf) -> int:
    agg_df = (
        sdf.filter(F.col("order_status") == "delivered")
        .groupBy("order_year", "order_month", "customer_state", "product_category")
        .agg(
            F.sum("order_value").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
            F.avg("delivery_days").alias("avg_delivery_days"),
            F.avg("is_late_delivery").alias("late_rate"),
            F.avg("review_score").alias("avg_review"),
        )
    )
    return agg_df.count()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("PandasVsSparkBenchmark").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    cols = [
        "order_id",
        "order_status",
        "order_year",
        "order_month",
        "customer_state",
        "product_category",
        "order_value",
        "delivery_days",
        "is_late_delivery",
        "review_score",
    ]
    fact_sdf = spark.read.parquet(args.fact_path).select(*cols).cache()
    rows_1x = fact_sdf.count()

    # Materialize Pandas 1x DataFrame outside the timed aggregation window
    pdf_1x = fact_sdf.toPandas()

    # 1x Benchmark
    t0 = time.perf_counter()
    _ = run_pandas_agg(pdf_1x)
    pandas_1x_sec = time.perf_counter() - t0

    t0 = time.perf_counter()
    _ = run_spark_agg(fact_sdf)
    spark_1x_sec = time.perf_counter() - t0

    # 10x Benchmark
    pdf_10x = pd.concat([pdf_1x] * 10, ignore_index=True)
    rows_10x = len(pdf_10x)
    fact_10x_sdf = fact_sdf
    for _ in range(9):
        fact_10x_sdf = fact_10x_sdf.unionAll(fact_sdf)
    fact_10x_sdf = fact_10x_sdf.cache()
    _ = fact_10x_sdf.count()

    t0 = time.perf_counter()
    _ = run_pandas_agg(pdf_10x)
    pandas_10x_sec = time.perf_counter() - t0

    t0 = time.perf_counter()
    _ = run_spark_agg(fact_10x_sdf)
    spark_10x_sec = time.perf_counter() - t0

    results = pd.DataFrame([
        {
            "scale": "1x (Full Olist)",
            "rows": rows_1x,
            "pandas_sec": round(pandas_1x_sec, 4),
            "spark_sec": round(spark_1x_sec, 4),
            "speedup_x": round(pandas_1x_sec / spark_1x_sec, 2) if spark_1x_sec > 0 else None,
        },
        {
            "scale": "10x (Duplicated)",
            "rows": rows_10x,
            "pandas_sec": round(pandas_10x_sec, 4),
            "spark_sec": round(spark_10x_sec, 4),
            "speedup_x": round(pandas_10x_sec / spark_10x_sec, 2) if spark_10x_sec > 0 else None,
        },
    ])

    os.makedirs(args.output_dir, exist_ok=True)
    csv_path = os.path.join(args.output_dir, "benchmark_results.csv")
    md_path = os.path.join(args.output_dir, "benchmark_results.md")
    results.to_csv(csv_path, index=False)

    md_table = results.to_markdown(index=False)
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("# Pandas vs. Spark Benchmark Results\n\n")
        f.write(md_table + "\n")

    print("\n=== Benchmark Results ===")
    print(md_table)
    spark.stop()


if __name__ == "__main__":
    main()

