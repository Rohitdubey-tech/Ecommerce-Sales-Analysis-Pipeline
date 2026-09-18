#!/usr/bin/env bash
set -euo pipefail

# Ingest Olist raw CSV files from local data/raw/ into HDFS /data/olist/raw/

LOCAL_RAW_DIR="${1:-data/raw}"
HDFS_RAW_DIR="/data/olist/raw"

EXPECTED_FILES=(
  "olist_customers_dataset.csv"
  "olist_geolocation_dataset.csv"
  "olist_order_items_dataset.csv"
  "olist_order_payments_dataset.csv"
  "olist_order_reviews_dataset.csv"
  "olist_orders_dataset.csv"
  "olist_products_dataset.csv"
  "olist_sellers_dataset.csv"
  "product_category_name_translation.csv"
)

echo "Checking local raw CSV files in ${LOCAL_RAW_DIR}..."
for f in "${EXPECTED_FILES[@]}"; do
  if [[ ! -f "${LOCAL_RAW_DIR}/${f}" ]]; then
    echo "ERROR: Missing ${LOCAL_RAW_DIR}/${f}"
    echo "Download the dataset from Kaggle (olistbr/brazilian-ecommerce) and unzip into ${LOCAL_RAW_DIR}/"
    exit 1
  fi
done
echo "All 9 CSV files found locally."

echo "Creating HDFS directory ${HDFS_RAW_DIR}..."
hdfs dfs -mkdir -p "${HDFS_RAW_DIR}"

echo "Uploading CSVs to HDFS..."
hdfs dfs -put -f "${LOCAL_RAW_DIR}"/*.csv "${HDFS_RAW_DIR}/"

echo "Verifying HDFS contents:"
hdfs dfs -ls -h "${HDFS_RAW_DIR}"
echo "Row counts (including header):"
for f in "${EXPECTED_FILES[@]}"; do
  count=$(hdfs dfs -cat "${HDFS_RAW_DIR}/${f}" | wc -l | tr -d ' ')
  printf "  %-45s %s lines\n" "${f}" "${count}"
done

