-- Create Hive database and EXTERNAL tables over Spark Parquet outputs

CREATE DATABASE IF NOT EXISTS olist_analytics;
USE olist_analytics;

CREATE EXTERNAL TABLE IF NOT EXISTS fact_order_items (
    order_id STRING,
    order_item_id INT,
    customer_id STRING,
    customer_unique_id STRING,
    product_id STRING,
    seller_id STRING,
    order_status STRING,
    order_purchase_timestamp TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    price DOUBLE,
    freight_value DOUBLE,
    order_value DOUBLE,
    order_total_payment DOUBLE,
    primary_payment_type STRING,
    max_installments INT,
    review_score DOUBLE,
    delivery_days INT,
    is_late_delivery INT,
    customer_state STRING,
    seller_state STRING,
    product_category STRING,
    order_weekday STRING
)
PARTITIONED BY (order_year INT, order_month INT)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/data/olist/processed/fact_order_items';

-- Discover year/month partitions created by Spark
MSCK REPAIR TABLE fact_order_items;

CREATE EXTERNAL TABLE IF NOT EXISTS dim_customers (
    customer_id STRING,
    customer_unique_id STRING,
    customer_zip_code_prefix STRING,
    customer_city STRING,
    customer_state STRING,
    customer_lat DOUBLE,
    customer_lng DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/data/olist/processed/dim_customers';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_products (
    product_id STRING,
    product_category STRING,
    product_weight_g DOUBLE,
    product_length_cm DOUBLE,
    product_height_cm DOUBLE,
    product_width_cm DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/data/olist/processed/dim_products';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_sellers (
    seller_id STRING,
    seller_zip_code_prefix STRING,
    seller_city STRING,
    seller_state STRING,
    seller_lat DOUBLE,
    seller_lng DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/data/olist/processed/dim_sellers';

SHOW TABLES;
SHOW PARTITIONS fact_order_items;

