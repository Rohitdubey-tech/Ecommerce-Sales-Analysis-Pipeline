"""RFM Customer Segmentation using Spark (feature prep) + Scikit-learn K-Means.

Computes Recency, Frequency, Monetary per unique customer, evaluates k=2..7 with
Elbow (inertia) and Silhouette score, plots diagnostics, assigns business segment
names, and exports segment summaries for Power BI.
"""

import argparse
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Olist RFM K-Means Segmentation")
    parser.add_argument(
        "--fact-path",
        default="hdfs://localhost:9000/data/olist/processed/fact_order_items",
        help="Path to fact_order_items Parquet (HDFS or local).",
    )
    parser.add_argument(
        "--dashboard-dir",
        default="dashboard",
        help="Directory to export RFM CSVs for Power BI.",
    )
    parser.add_argument(
        "--docs-dir",
        default="docs",
        help="Directory to save Elbow/Silhouette diagnostic chart.",
    )
    parser.add_argument(
        "--k",
        type=int,
        default=0,
        help="Optional fixed number of clusters. If 0, auto-selects k with highest silhouette score.",
    )
    return parser.parse_args()


def assign_segment_names(summary: pd.DataFrame) -> dict[int, str]:
    """Map cluster IDs to business-friendly RFM names based on centroid ranks."""
    ranked = summary.copy()
    # Lower recency is better, higher frequency & monetary are better
    ranked["rfm_score"] = (
        (-ranked["avg_recency"]).rank()
        + ranked["avg_frequency"].rank()
        + ranked["avg_monetary"].rank()
    )
    ordered = ranked.sort_values("rfm_score", ascending=False).reset_index(drop=True)

    labels_pool = [
        "Champions",
        "Loyal Customers",
        "Potential Loyalists",
        "At Risk",
        "Hibernating",
        "Need Attention",
        "Lost",
    ]
    mapping = {}
    for idx, row in ordered.iterrows():
        cluster_id = int(row["cluster"])
        if row["avg_frequency"] >= 1.5 and idx == 0:
            mapping[cluster_id] = "Champions"
        elif row["avg_recency"] > ordered["avg_recency"].median() and idx >= len(ordered) - 2:
            mapping[cluster_id] = "Hibernating" if idx == len(ordered) - 1 else "At Risk"
        else:
            mapping[cluster_id] = labels_pool[min(idx, len(labels_pool) - 1)]
    return mapping


def main() -> None:
    args = parse_args()
    os.makedirs(args.dashboard_dir, exist_ok=True)
    os.makedirs(args.docs_dir, exist_ok=True)

    spark = SparkSession.builder.appName("OlistRFMSegmentation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    fact = spark.read.parquet(args.fact_path).filter(
        (F.col("order_status") == "delivered") & F.col("customer_unique_id").isNotNull()
    )

    max_date = fact.agg(F.max("order_purchase_timestamp")).collect()[0][0]
    rfm_sdf = fact.groupBy("customer_unique_id").agg(
        F.datediff(F.lit(max_date), F.max("order_purchase_timestamp")).alias("recency"),
        F.countDistinct("order_id").alias("frequency"),
        F.round(F.sum("order_value"), 2).alias("monetary"),
    )

    rfm_pdf = rfm_sdf.toPandas()
    spark.stop()

    print(f"Total unique delivered customers for RFM: {len(rfm_pdf)}")

    # Log-transform skewed Frequency & Monetary before StandardScaler
    features = pd.DataFrame({
        "recency": rfm_pdf["recency"].clip(lower=0),
        "frequency": np.log1p(rfm_pdf["frequency"].clip(lower=0)),
        "monetary": np.log1p(rfm_pdf["monetary"].clip(lower=0)),
    })
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(features)

    k_values = list(range(2, 8))
    inertias = []
    silhouettes = []

    for k in k_values:
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = km.fit_predict(X_scaled)
        inertias.append(km.inertia_)
        sil = silhouette_score(X_scaled, labels, sample_size=min(10000, len(X_scaled)), random_state=42)
        silhouettes.append(sil)
        print(f"k={k} | inertia={km.inertia_:,.1f} | silhouette={sil:.4f}")

    # Plot Elbow & Silhouette
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))
    ax1.plot(k_values, inertias, marker="o")
    ax1.set_title("Elbow Method (Inertia vs. k)")
    ax1.set_xlabel("Number of Clusters (k)")
    ax1.set_ylabel("Inertia")
    ax1.grid(True, alpha=0.3)

    ax2.plot(k_values, silhouettes, marker="o", color="darkorange")
    ax2.set_title("Silhouette Score vs. k")
    ax2.set_xlabel("Number of Clusters (k)")
    ax2.set_ylabel("Silhouette Score")
    ax2.grid(True, alpha=0.3)

    plot_path = os.path.join(args.docs_dir, "rfm_elbow_silhouette.png")
    plt.tight_layout()
    plt.savefig(plot_path, dpi=150)
    plt.close()

    best_k = args.k if args.k > 1 else k_values[int(np.argmax(silhouettes))]
    print(f"\nSelected k={best_k} (best silhouette={max(silhouettes):.4f})")

    final_km = KMeans(n_clusters=best_k, random_state=42, n_init=10)
    rfm_pdf["cluster"] = final_km.fit_predict(X_scaled)

    summary = (
        rfm_pdf.groupby("cluster", as_index=False)
        .agg(
            customer_count=("customer_unique_id", "count"),
            avg_recency=("recency", "mean"),
            avg_frequency=("frequency", "mean"),
            avg_monetary=("monetary", "mean"),
            total_revenue=("monetary", "sum"),
        )
    )
    summary["customer_pct"] = (100.0 * summary["customer_count"] / summary["customer_count"].sum()).round(2)
    summary["revenue_pct"] = (100.0 * summary["total_revenue"] / summary["total_revenue"].sum()).round(2)

    seg_names = assign_segment_names(summary)
    summary["segment_name"] = summary["cluster"].map(seg_names)
    rfm_pdf["segment_name"] = rfm_pdf["cluster"].map(seg_names)

    summary = summary.round(2).sort_values("total_revenue", ascending=False)
    print("\n=== RFM Segment Summary ===")
    print(summary.to_markdown(index=False))

    summary.to_csv(os.path.join(args.dashboard_dir, "rfm_segments.csv"), index=False)
    rfm_pdf.to_csv(os.path.join(args.dashboard_dir, "rfm_customer_assignments.csv"), index=False)
    print(f"\nSaved RFM outputs to {args.dashboard_dir}/ and {plot_path}")


if __name__ == "__main__":
    main()

