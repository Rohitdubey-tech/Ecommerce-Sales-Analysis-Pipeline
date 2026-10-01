# Resume Bullet Points

Format: **Action verb + what + tech + measurable result**

1. **Engineered** an end-to-end batch e-commerce analytics pipeline ingesting 9 Olist CSVs (`1,555,860` raw rows) into **HDFS**, transforming and partitioning star-schema **Parquet** tables (`103,892` cleaned fact rows) in **PySpark & Scala**, and exposing 9 **Hive** SQL analytical views with window functions (`LAG`, `RANK`, `DENSE_RANK`).
2. **Benchmarked** multi-dimensional aggregations in **Pandas vs. Apache Spark** at 1x (`103,892` rows: `0.05s` vs. `0.53s`) and 10x (`1,038,920` rows: `0.26s` vs. `1.92s`), quantifying Spark's scheduling overhead vs. horizontal scalability and exporting 6 aggregated datasets for a 2-page **Power BI** dashboard (`R$ 10.85M` revenue, `88,778` orders).
3. **Built** an **RFM customer segmentation** model using **PySpark** and **Scikit-learn K-Means** across `85,967` unique customers (`k=2`, silhouette `0.7127`), identifying a `2.94%` **Champions** cohort (`2.11` avg orders, `R$ 258.76` avg spend) driving `6.02%` of revenue and pinpointing a `2.50`-star review drop on deliveries delayed `8+` days.
