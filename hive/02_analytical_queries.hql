-- Analytical SQL queries for Olist E-Commerce Analytics (Hive)
USE olist_analytics;

-- 1. Executive KPIs
SELECT
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(*) AS total_order_items,
    ROUND(SUM(order_value), 2) AS total_revenue,
    ROUND(SUM(order_value) / COUNT(DISTINCT order_id), 2) AS avg_order_value,
    ROUND(AVG(review_score), 2) AS avg_review_score,
    ROUND(100.0 * AVG(CAST(is_late_delivery AS DOUBLE)), 2) AS late_delivery_pct
FROM fact_order_items
WHERE order_status = 'delivered';

-- 2. Monthly Revenue + MoM Growth % (Window Function: LAG)
WITH monthly AS (
    SELECT
        order_year,
        order_month,
        COUNT(DISTINCT order_id) AS orders_count,
        ROUND(SUM(order_value), 2) AS revenue
    FROM fact_order_items
    WHERE order_status = 'delivered'
    GROUP BY order_year, order_month
)
SELECT
    order_year,
    order_month,
    orders_count,
    revenue,
    LAG(revenue) OVER (ORDER BY order_year, order_month) AS prev_month_revenue,
    ROUND(
        100.0 * (revenue - LAG(revenue) OVER (ORDER BY order_year, order_month))
        / NULLIF(LAG(revenue) OVER (ORDER BY order_year, order_month), 0),
        2
    ) AS mom_growth_pct
FROM monthly
ORDER BY order_year, order_month;

-- 3. Top 10 Product Categories by Revenue + Share % (Window Functions: RANK, SUM OVER)
WITH cat_rev AS (
    SELECT
        product_category,
        COUNT(DISTINCT order_id) AS orders_count,
        ROUND(SUM(order_value), 2) AS revenue,
        ROUND(AVG(review_score), 2) AS avg_review_score
    FROM fact_order_items
    WHERE order_status = 'delivered'
    GROUP BY product_category
)
SELECT
    RANK() OVER (ORDER BY revenue DESC) AS category_rank,
    product_category,
    orders_count,
    revenue,
    ROUND(100.0 * revenue / SUM(revenue) OVER (), 2) AS revenue_share_pct,
    avg_review_score
FROM cat_rev
ORDER BY revenue DESC
LIMIT 10;

-- 4. Late-Delivery Rate and Avg Delivery Days by Customer State
SELECT
    customer_state,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(AVG(delivery_days), 2) AS avg_delivery_days,
    ROUND(100.0 * AVG(CAST(is_late_delivery AS DOUBLE)), 2) AS late_delivery_pct,
    ROUND(AVG(review_score), 2) AS avg_review_score
FROM fact_order_items
WHERE order_status = 'delivered' AND delivery_days IS NOT NULL
GROUP BY customer_state
HAVING COUNT(DISTINCT order_id) >= 100
ORDER BY late_delivery_pct DESC;

-- 5. Review Score vs. Delivery Delay Bucket
WITH order_delivery AS (
    SELECT DISTINCT
        order_id,
        review_score,
        DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) AS delay_days
    FROM fact_order_items
    WHERE order_status = 'delivered'
      AND order_delivered_customer_date IS NOT NULL
      AND order_estimated_delivery_date IS NOT NULL
      AND review_score IS NOT NULL
)
SELECT
    CASE
        WHEN delay_days <= 0 THEN 'On-time / Early'
        WHEN delay_days BETWEEN 1 AND 3 THEN '1-3 Days Late'
        WHEN delay_days BETWEEN 4 AND 7 THEN '4-7 Days Late'
        ELSE '8+ Days Late'
    END AS delivery_bucket,
    COUNT(*) AS orders_count,
    ROUND(AVG(review_score), 2) AS avg_review_score
FROM order_delivery
GROUP BY
    CASE
        WHEN delay_days <= 0 THEN 'On-time / Early'
        WHEN delay_days BETWEEN 1 AND 3 THEN '1-3 Days Late'
        WHEN delay_days BETWEEN 4 AND 7 THEN '4-7 Days Late'
        ELSE '8+ Days Late'
    END
ORDER BY avg_review_score DESC;

-- 6. Payment Method Mix (Window Function: SUM OVER)
WITH pay_mix AS (
    SELECT
        COALESCE(primary_payment_type, 'unknown') AS payment_type,
        COUNT(DISTINCT order_id) AS orders_count,
        ROUND(SUM(order_value), 2) AS revenue,
        ROUND(AVG(max_installments), 2) AS avg_installments
    FROM fact_order_items
    WHERE order_status = 'delivered'
    GROUP BY COALESCE(primary_payment_type, 'unknown')
)
SELECT
    payment_type,
    orders_count,
    revenue,
    ROUND(100.0 * revenue / SUM(revenue) OVER (), 2) AS revenue_share_pct,
    avg_installments
FROM pay_mix
ORDER BY revenue DESC;

-- 7. Repeat-Customer Rate
WITH cust_orders AS (
    SELECT
        customer_unique_id,
        COUNT(DISTINCT order_id) AS order_count
    FROM fact_order_items
    WHERE order_status = 'delivered'
    GROUP BY customer_unique_id
)
SELECT
    COUNT(*) AS total_unique_customers,
    SUM(CASE WHEN order_count >= 2 THEN 1 ELSE 0 END) AS repeat_customers,
    ROUND(100.0 * SUM(CASE WHEN order_count >= 2 THEN 1 ELSE 0 END) / COUNT(*), 2) AS repeat_customer_pct
FROM cust_orders;

-- 8. Top 15 Sellers by Revenue with Ranking (Window Function: DENSE_RANK)
WITH seller_perf AS (
    SELECT
        seller_id,
        MAX(seller_state) AS seller_state,
        COUNT(DISTINCT order_id) AS orders_count,
        ROUND(SUM(order_value), 2) AS total_revenue,
        ROUND(AVG(review_score), 2) AS avg_review_score,
        ROUND(100.0 * AVG(CAST(is_late_delivery AS DOUBLE)), 2) AS late_delivery_pct
    FROM fact_order_items
    WHERE order_status = 'delivered'
    GROUP BY seller_id
)
SELECT
    DENSE_RANK() OVER (ORDER BY total_revenue DESC) AS seller_rank,
    seller_id,
    seller_state,
    orders_count,
    total_revenue,
    avg_review_score,
    late_delivery_pct
FROM seller_perf
ORDER BY total_revenue DESC
LIMIT 15;

-- 9. Weekday Order & Revenue Pattern
SELECT
    order_weekday,
    COUNT(DISTINCT order_id) AS orders_count,
    ROUND(SUM(order_value), 2) AS total_revenue,
    ROUND(SUM(order_value) / COUNT(DISTINCT order_id), 2) AS avg_order_value
FROM fact_order_items
WHERE order_status = 'delivered'
GROUP BY order_weekday
ORDER BY total_revenue DESC;

