# Power BI Dashboard Design (2-Page Blueprint)

## Running Power BI on macOS (Apple Silicon)
- **Recommended:** Install **Windows 11 ARM** in **UTM** (free) or **Parallels**, then install **Power BI Desktop** to build the `.pbix` file and author DAX measures.
- **Web option:** Publish `.pbix` to **Power BI Service** (`app.powerbi.com`) for viewing and sharing.
- **Native Mac fallback:** **Tableau Public** (free, runs natively on Apple Silicon) can import the exact same CSVs from `dashboard/`.

---

## Data Sources (exported by `spark_jobs/export_dashboard_csvs.py` & `rfm_segmentation.py`)
Load these CSVs from `dashboard/`:
1. `kpi_summary.csv`
2. `monthly_revenue.csv`
3. `category_revenue.csv`
4. `state_delivery_metrics.csv`
5. `delay_vs_review.csv`
6. `payment_mix.csv`
7. `rfm_segments.csv`

---

## DAX Measures to Create
```dax
Total Revenue = SUM(monthly_revenue[revenue])

Total Orders = SUM(monthly_revenue[orders_count])

Average Order Value (AOV) = DIVIDE([Total Revenue], [Total Orders], 0)

Weighted Avg Review Score =
DIVIDE(
    SUMX(state_delivery_metrics, state_delivery_metrics[avg_review_score] * state_delivery_metrics[orders_count]),
    SUM(state_delivery_metrics[orders_count]),
    0
)
```

---

## Page 1 – Sales Overview
1. **4 KPI Cards (Top Row)**
   - **Total Revenue**: `[Total Revenue]` (Format: `R$ #,##0`)
   - **Total Orders**: `[Total Orders]` (Format: `#,##0`)
   - **AOV**: `[Average Order Value (AOV)]` (Format: `R$ #,##0.00`)
   - **Avg Review Score**: `[Weighted Avg Review Score]` (Format: `0.00`)
2. **Line Chart – Monthly Revenue Trend**
   - X-axis: `monthly_revenue[order_year]`, `monthly_revenue[order_month]`
   - Y-axis: `[Total Revenue]`
   - Tooltip: `monthly_revenue[orders_count]`, `monthly_revenue[avg_review_score]`
3. **Horizontal Bar Chart – Top 10 Product Categories by Revenue**
   - Y-axis: `category_revenue[product_category]` (Top N filter = 10 by `revenue`)
   - X-axis: `category_revenue[revenue]`
   - Tooltip: `category_revenue[orders_count]`, `category_revenue[avg_review_score]`
4. **Filled Map / Shape Map – Revenue by Customer State (Brazil)**
   - Location: `state_delivery_metrics[customer_state]` (Data Category: State or Province)
   - Color saturation: `state_delivery_metrics[revenue]`
   - Tooltip: `orders_count`, `late_delivery_pct`, `avg_delivery_days`

---

## Page 2 – Customers & Delivery
1. **Treemap or Clustered Bar Chart – RFM Customer Segments**
   - Category / Y-axis: `rfm_segments[segment_name]`
   - Values / X-axis: `rfm_segments[customer_count]` and `rfm_segments[total_revenue]`
   - Tooltip: `avg_recency`, `avg_frequency`, `avg_monetary`, `revenue_pct`
2. **Clustered Column Chart – Late Delivery vs. Review Score**
   - X-axis: `delay_vs_review[delivery_bucket]` (Sort order: On-time / Early -> 1-3 Days Late -> 4-7 Days Late -> 8+ Days Late)
   - Y-axis: `delay_vs_review[avg_review_score]`
   - Tooltip: `delay_vs_review[orders_count]`
3. **Combo Chart (Bar + Line) – Delivery Time & Late Rate by State**
   - X-axis: `state_delivery_metrics[customer_state]`
   - Column Y-axis: `state_delivery_metrics[avg_delivery_days]`
   - Line Y-axis: `state_delivery_metrics[late_delivery_pct]`
4. **Donut Chart – Payment Method Mix**
   - Legend: `payment_mix[payment_type]`
   - Values: `payment_mix[revenue]`
   - Tooltip: `payment_mix[orders_count]`, `payment_mix[avg_installments]`

