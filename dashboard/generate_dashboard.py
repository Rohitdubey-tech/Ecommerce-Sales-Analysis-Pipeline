"""Build an interactive 2-page HTML dashboard (dashboard/index.html) and render
high-resolution 2-page executive dashboard PNGs (docs/dashboard_page1.png,
docs/dashboard_page2.png) directly from the exported dashboard/*.csv files.
"""

import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import pandas as pd

DASH_DIR = "dashboard"
DOCS_DIR = "docs"
os.makedirs(DOCS_DIR, exist_ok=True)

kpi = pd.read_csv(os.path.join(DASH_DIR, "kpi_summary.csv")).iloc[0]
monthly = pd.read_csv(os.path.join(DASH_DIR, "monthly_revenue.csv"))
monthly["ym"] = monthly["order_year"].astype(str) + "-" + monthly["order_month"].astype(str).str.zfill(2)
cats = pd.read_csv(os.path.join(DASH_DIR, "category_revenue.csv")).head(10)
states = pd.read_csv(os.path.join(DASH_DIR, "state_delivery_metrics.csv"))
delay = pd.read_csv(os.path.join(DASH_DIR, "delay_vs_review.csv"))
pay = pd.read_csv(os.path.join(DASH_DIR, "payment_mix.csv"))
rfm = pd.read_csv(os.path.join(DASH_DIR, "rfm_segments.csv"))

bucket_order = ["On-time / Early", "1-3 Days Late", "4-7 Days Late", "8+ Days Late"]
delay["delivery_bucket"] = pd.Categorical(delay["delivery_bucket"], categories=bucket_order, ordered=True)
delay = delay.sort_values("delivery_bucket")


def render_page1_png() -> None:
    fig = plt.figure(figsize=(16, 9), facecolor="#F3F4F6")
    gs = gridspec.GridSpec(3, 2, height_ratios=[0.9, 1.8, 2.0], hspace=0.42, wspace=0.25)

    fig.suptitle(
        "Olist E-Commerce Analytics — Page 1: Sales Overview",
        fontsize=18, fontweight="bold", color="#111827", y=0.97,
    )

    # Top KPI row spanning both columns
    kpi_gs = gridspec.GridSpecFromSubplotSpec(1, 4, subplot_spec=gs[0, :], wspace=0.18)
    cards = [
        ("Total Revenue", f"R$ {kpi['total_revenue']:,.0f}", "#1D4ED8"),
        ("Total Orders", f"{int(kpi['total_orders']):,}", "#0F766E"),
        ("Avg Order Value (AOV)", f"R$ {kpi['avg_order_value']:,.2f}", "#7C3AED"),
        ("Avg Review Score", f"{kpi['avg_review_score']:.2f} / 5.0", "#D97706"),
    ]
    for idx, (label, val, color) in enumerate(cards):
        ax = fig.add_subplot(kpi_gs[0, idx])
        ax.set_facecolor("white")
        ax.set_xticks([])
        ax.set_yticks([])
        for spine in ax.spines.values():
            spine.set_color("#E5E7EB")
        ax.text(0.5, 0.65, val, ha="center", va="center", fontsize=18, fontweight="bold", color=color)
        ax.text(0.5, 0.28, label, ha="center", va="center", fontsize=11, color="#4B5563")

    # Monthly Revenue Trend (row 1, full width)
    ax_m = fig.add_subplot(gs[1, :])
    ax_m.set_facecolor("white")
    ax_m.plot(monthly["ym"], monthly["revenue"] / 1000.0, marker="o", color="#1D4ED8", linewidth=2.2)
    ax_m.fill_between(range(len(monthly)), monthly["revenue"] / 1000.0, color="#DBEAFE", alpha=0.5)
    ax_m.set_title("Monthly Revenue Trend (R$ Thousands)", fontsize=12, fontweight="bold", color="#1F2937")
    ax_m.set_ylabel("Revenue (R$ K)")
    ax_m.tick_params(axis="x", rotation=45, labelsize=8)
    ax_m.grid(axis="y", linestyle="--", alpha=0.4)

    # Top 10 Categories (row 2, col 0)
    ax_c = fig.add_subplot(gs[2, 0])
    ax_c.set_facecolor("white")
    cats_s = cats.sort_values("revenue")
    ax_c.barh(cats_s["product_category"], cats_s["revenue"] / 1000.0, color="#2563EB")
    ax_c.set_title("Top 10 Product Categories (R$ Thousands)", fontsize=12, fontweight="bold", color="#1F2937")
    ax_c.set_xlabel("Revenue (R$ K)")
    ax_c.tick_params(axis="y", labelsize=9)
    ax_c.grid(axis="x", linestyle="--", alpha=0.4)

    # Top 10 States by Revenue (row 2, col 1)
    ax_s = fig.add_subplot(gs[2, 1])
    ax_s.set_facecolor("white")
    top_st = states.head(10).sort_values("revenue")
    ax_s.barh(top_st["customer_state"], top_st["revenue"] / 1000.0, color="#0F766E")
    ax_s.set_title("Top 10 Customer States by Revenue (R$ Thousands)", fontsize=12, fontweight="bold", color="#1F2937")
    ax_s.set_xlabel("Revenue (R$ K)")
    ax_s.grid(axis="x", linestyle="--", alpha=0.4)

    plt.savefig(os.path.join(DOCS_DIR, "dashboard_page1.png"), dpi=160, bbox_inches="tight")
    plt.close()


def render_page2_png() -> None:
    fig = plt.figure(figsize=(16, 9), facecolor="#F3F4F6")
    gs = gridspec.GridSpec(2, 2, hspace=0.35, wspace=0.25)

    fig.suptitle(
        "Olist E-Commerce Analytics — Page 2: Customers & Delivery",
        fontsize=18, fontweight="bold", color="#111827", y=0.97,
    )

    # RFM Segments (top-left)
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.set_facecolor("white")
    bars = ax1.bar(rfm["segment_name"], rfm["customer_count"], color=["#94A3B8", "#059669"])
    ax1.set_title("RFM Customer Segments (Unique Customers)", fontsize=12, fontweight="bold", color="#1F2937")
    ax1.set_ylabel("Customers")
    for bar, (_, row) in zip(bars, rfm.iterrows()):
        ax1.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 1200,
            f"{int(row['customer_count']):,} ({row['customer_pct']}%)\nAvg: R$ {row['avg_monetary']:.0f}",
            ha="center", va="bottom", fontsize=9, fontweight="bold",
        )
    ax1.set_ylim(0, rfm["customer_count"].max() * 1.18)
    ax1.grid(axis="y", linestyle="--", alpha=0.4)

    # Delivery Delay vs Review Score (top-right)
    ax2 = fig.add_subplot(gs[0, 1])
    ax2.set_facecolor("white")
    colors = ["#10B981", "#F59E0B", "#F97316", "#EF4444"]
    bars2 = ax2.bar(delay["delivery_bucket"].astype(str), delay["avg_review_score"], color=colors)
    ax2.set_title("Avg Review Score by Delivery Delay Bucket", fontsize=12, fontweight="bold", color="#1F2937")
    ax2.set_ylabel("Avg Review Score (1–5)")
    ax2.set_ylim(0, 5.0)
    for bar in bars2:
        ax2.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.1,
            f"{bar.get_height():.2f}",
            ha="center", va="bottom", fontsize=10, fontweight="bold",
        )
    ax2.grid(axis="y", linestyle="--", alpha=0.4)

    # Delivery Time & Late Rate by Top 12 States (bottom-left)
    ax3 = fig.add_subplot(gs[1, 0])
    ax3.set_facecolor("white")
    st12 = states.head(12)
    ax3.bar(st12["customer_state"], st12["avg_delivery_days"], color="#3B82F6", label="Avg Delivery Days")
    ax3.set_title("Delivery Days & Late Rate % (Top 12 States)", fontsize=12, fontweight="bold", color="#1F2937")
    ax3.set_ylabel("Avg Delivery Days", color="#1D4ED8")
    ax3_t = ax3.twinx()
    ax3_t.plot(st12["customer_state"], st12["late_delivery_pct"], color="#DC2626", marker="o", linewidth=2, label="Late %")
    ax3_t.set_ylabel("Late Delivery %", color="#DC2626")
    ax3.grid(axis="y", linestyle="--", alpha=0.4)

    # Payment Method Mix (bottom-right)
    ax4 = fig.add_subplot(gs[1, 1])
    ax4.set_facecolor("white")
    pay_top = pay[pay["payment_type"] != "unknown"]
    ax4.pie(
        pay_top["revenue"],
        labels=pay_top["payment_type"],
        autopct="%1.1f%%",
        startangle=140,
        colors=["#1D4ED8", "#0F766E", "#7C3AED", "#D97706"],
        wedgeprops={"width": 0.45},
    )
    ax4.set_title("Revenue Share by Payment Method", fontsize=12, fontweight="bold", color="#1F2937")

    plt.savefig(os.path.join(DOCS_DIR, "dashboard_page2.png"), dpi=160, bbox_inches="tight")
    plt.close()


def build_interactive_html() -> None:
    payload = {
        "kpi": {k: (float(v) if isinstance(v, (int, float)) or hasattr(v, "item") else v) for k, v in kpi.items()},
        "monthly": monthly[["ym", "revenue", "orders_count", "avg_review_score"]].to_dict(orient="records"),
        "categories": cats.to_dict(orient="records"),
        "states": states.head(15).to_dict(orient="records"),
        "delay": delay.assign(delivery_bucket=delay["delivery_bucket"].astype(str)).to_dict(orient="records"),
        "payment": pay[pay["payment_type"] != "unknown"].to_dict(orient="records"),
        "rfm": rfm.to_dict(orient="records"),
    }

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Olist E-Commerce Analytics Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; background: #f3f4f6; color: #111827; }}
    header {{ background: #1e293b; color: #fff; padding: 16px 28px; display: flex; justify-content: space-between; align-items: center; }}
    header h1 {{ margin: 0; font-size: 20px; }}
    .tabs button {{ background: #334155; color: #fff; border: none; padding: 8px 16px; margin-left: 8px; border-radius: 6px; cursor: pointer; font-weight: 600; }}
    .tabs button.active {{ background: #2563eb; }}
    .page {{ display: none; padding: 24px 28px; max-width: 1320px; margin: auto; }}
    .page.active {{ display: block; }}
    .kpis {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 20px; }}
    .card {{ background: #fff; border-radius: 10px; padding: 18px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
    .kpi-val {{ font-size: 26px; font-weight: 700; color: #1d4ed8; }}
    .kpi-lbl {{ font-size: 13px; color: #6b7280; margin-top: 4px; }}
    .grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-top: 16px; }}
    canvas {{ max-height: 320px; }}
  </style>
</head>
<body>
  <header>
    <h1>Olist E-Commerce Sales Analytics</h1>
    <div class="tabs">
      <button id="btn-p1" class="active" onclick="showPage(1)">Page 1: Sales Overview</button>
      <button id="btn-p2" onclick="showPage(2)">Page 2: Customers &amp; Delivery</button>
    </div>
  </header>

  <section id="page1" class="page active">
    <div class="kpis">
      <div class="card"><div class="kpi-val">R$ {kpi['total_revenue']:,.0f}</div><div class="kpi-lbl">Total Revenue</div></div>
      <div class="card"><div class="kpi-val">{int(kpi['total_orders']):,}</div><div class="kpi-lbl">Delivered Orders</div></div>
      <div class="card"><div class="kpi-val">R$ {kpi['avg_order_value']:,.2f}</div><div class="kpi-lbl">Average Order Value (AOV)</div></div>
      <div class="card"><div class="kpi-val">{kpi['avg_review_score']:.2f} / 5.0</div><div class="kpi-lbl">Avg Review Score</div></div>
    </div>
    <div class="card"><canvas id="chartMonthly"></canvas></div>
    <div class="grid-2">
      <div class="card"><canvas id="chartCategories"></canvas></div>
      <div class="card"><canvas id="chartStates"></canvas></div>
    </div>
  </section>

  <section id="page2" class="page">
    <div class="grid-2">
      <div class="card"><canvas id="chartRfm"></canvas></div>
      <div class="card"><canvas id="chartDelay"></canvas></div>
      <div class="card"><canvas id="chartStateDelivery"></canvas></div>
      <div class="card"><canvas id="chartPayment"></canvas></div>
    </div>
  </section>

  <script>
    const DATA = {json.dumps(payload)};
    function showPage(n) {{
      document.getElementById('page1').classList.toggle('active', n === 1);
      document.getElementById('page2').classList.toggle('active', n === 2);
      document.getElementById('btn-p1').classList.toggle('active', n === 1);
      document.getElementById('btn-p2').classList.toggle('active', n === 2);
    }}

    new Chart(document.getElementById('chartMonthly'), {{
      type: 'line',
      data: {{
        labels: DATA.monthly.map(d => d.ym),
        datasets: [{{ label: 'Monthly Revenue (R$)', data: DATA.monthly.map(d => d.revenue), borderColor: '#2563eb', backgroundColor: 'rgba(37,99,235,0.12)', fill: true, tension: 0.25 }}]
      }}
    }});

    new Chart(document.getElementById('chartCategories'), {{
      type: 'bar',
      data: {{
        labels: DATA.categories.map(d => d.product_category),
        datasets: [{{ label: 'Top 10 Categories Revenue (R$)', data: DATA.categories.map(d => d.revenue), backgroundColor: '#2563eb' }}]
      }},
      options: {{ indexAxis: 'y' }}
    }});

    new Chart(document.getElementById('chartStates'), {{
      type: 'bar',
      data: {{
        labels: DATA.states.slice(0, 10).map(d => d.customer_state),
        datasets: [{{ label: 'Top 10 States Revenue (R$)', data: DATA.states.slice(0, 10).map(d => d.revenue), backgroundColor: '#0f766e' }}]
      }},
      options: {{ indexAxis: 'y' }}
    }});

    new Chart(document.getElementById('chartRfm'), {{
      type: 'bar',
      data: {{
        labels: DATA.rfm.map(d => d.segment_name),
        datasets: [{{ label: 'Customers by RFM Segment', data: DATA.rfm.map(d => d.customer_count), backgroundColor: ['#94a3b8', '#059669'] }}]
      }}
    }});

    new Chart(document.getElementById('chartDelay'), {{
      type: 'bar',
      data: {{
        labels: DATA.delay.map(d => d.delivery_bucket),
        datasets: [{{ label: 'Avg Review Score vs. Delivery Delay', data: DATA.delay.map(d => d.avg_review_score), backgroundColor: ['#10b981', '#f59e0b', '#f97316', '#ef4444'] }}]
      }},
      options: {{ scales: {{ y: {{ min: 0, max: 5 }} }} }}
    }});

    new Chart(document.getElementById('chartStateDelivery'), {{
      type: 'bar',
      data: {{
        labels: DATA.states.slice(0, 12).map(d => d.customer_state),
        datasets: [
          {{ label: 'Avg Delivery Days', data: DATA.states.slice(0, 12).map(d => d.avg_delivery_days), backgroundColor: '#3b82f6' }},
          {{ label: 'Late Delivery %', data: DATA.states.slice(0, 12).map(d => d.late_delivery_pct), type: 'line', borderColor: '#dc2626', tension: 0.2 }}
        ]
      }}
    }});

    new Chart(document.getElementById('chartPayment'), {{
      type: 'doughnut',
      data: {{
        labels: DATA.payment.map(d => d.payment_type),
        datasets: [{{ data: DATA.payment.map(d => d.revenue), backgroundColor: ['#1d4ed8', '#0f766e', '#7c3aed', '#d97706'] }}]
      }}
    }});
  </script>
</body>
</html>
"""
    with open(os.path.join(DASH_DIR, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)


if __name__ == "__main__":
    render_page1_png()
    render_page2_png()
    build_interactive_html()
    print("Generated dashboard/index.html, docs/dashboard_page1.png, and docs/dashboard_page2.png")
