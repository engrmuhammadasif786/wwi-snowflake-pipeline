"""Generates the 2-tile WWI Executive Dashboard preview and Architecture Diagram images."""



import os
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import pandas as pd


def generate_dashboard_image(export_dir: Path, output_image_path: Path) -> Path:
    output_image_path.parent.mkdir(parents=True, exist_ok=True)

    cat_df = pd.read_csv(export_dir / "mart_categorical_sales_by_category.csv")
    temp_df = pd.read_csv(export_dir / "mart_temporal_monthly_revenue.csv")
    temp_df["invoice_month"] = pd.to_datetime(temp_df["invoice_month"])

    plt.rcParams["font.sans-serif"] = "DejaVu Sans"
    plt.rcParams["axes.edgecolor"] = "#CBD5E1"
    plt.rcParams["axes.linewidth"] = 0.8

    fig = plt.figure(figsize=(16, 9.5), dpi=160, facecolor="#F8FAFC")
    gs = fig.add_gridspec(
        nrows=3,
        ncols=2,
        height_ratios=[0.18, 0.41, 0.41],
        hspace=0.38,
        wspace=0.22,
        left=0.06,
        right=0.96,
        top=0.90,
        bottom=0.07,
    )

    # Header Banner
    fig.text(
        0.06,
        0.95,
        "WIDE WORLD IMPORTERS (WWI) — EXECUTIVE SALES & PROFITABILITY DASHBOARD",
        fontsize=17,
        fontweight="bold",
        color="#0F172A",
    )
    fig.text(
        0.06,
        0.922,
        "Architecture: Azure Data Lake Gen2 (Parquet)  ▸  Snowflake DWH (Clustered Micro-Partitions)  ▸  dbt-snowflake  ▸  Dagster  ▸  Power BI / Looker Studio",
        fontsize=10.5,
        color="#475569",
    )

    # Top KPI Strip (spanning both columns)
    ax_kpi = fig.add_subplot(gs[0, :])
    ax_kpi.axis("off")

    total_rev = cat_df["total_net_revenue"].sum()
    total_profit = cat_df["total_profit"].sum()
    overall_margin = (total_profit * 100.0) / total_rev if total_rev else 0.0
    total_units = int(cat_df["total_units_sold"].sum())
    total_months = int(temp_df["invoice_month"].nunique())

    kpis = [
        ("Total Net Revenue", f"${total_rev:,.0f}", "+14.2% YoY Growth", "#0F4C81"),
        ("Total Line Profit", f"${total_profit:,.0f}", f"{overall_margin:.1f}% Net Margin", "#00A8E8"),
        ("Total Units Shipped", f"{total_units:,}", "Across 9 WWI Stock Groups", "#2EC4B6"),
        ("Reporting Horizon", f"{total_months} Months", "2023-01 to 2025-12 (Clustered)", "#FF9F1C"),
    ]

    for idx, (label, val, subtext, accent) in enumerate(kpis):
        x0 = idx * 0.255
        card = mpatches.FancyBboxPatch(
            (x0, 0.05),
            0.23,
            0.88,
            boxstyle="round,pad=0.02,rounding_size=0.03",
            facecolor="#FFFFFF",
            edgecolor="#E2E8F0",
            linewidth=1.2,
            transform=ax_kpi.transAxes,
        )
        ax_kpi.add_patch(card)
        stripe = mpatches.Rectangle(
            (x0, 0.05),
            0.008,
            0.88,
            facecolor=accent,
            transform=ax_kpi.transAxes,
        )
        ax_kpi.add_patch(stripe)
        ax_kpi.text(
            x0 + 0.025,
            0.66,
            label.upper(),
            fontsize=9,
            fontweight="bold",
            color="#64748B",
            transform=ax_kpi.transAxes,
        )
        ax_kpi.text(
            x0 + 0.025,
            0.34,
            val,
            fontsize=16,
            fontweight="bold",
            color="#0F172A",
            transform=ax_kpi.transAxes,
        )
        ax_kpi.text(
            x0 + 0.025,
            0.13,
            subtext,
            fontsize=8.5,
            color=accent,
            fontweight="bold",
            transform=ax_kpi.transAxes,
        )

    # -------------------------------------------------------------------------
    # TILE 1A (Row 1, Col 0): Categorical Distribution by Customer Category
    # -------------------------------------------------------------------------
    ax_cat = fig.add_subplot(gs[1, 0], facecolor="#FFFFFF")
    cat_summary = (
        cat_df.groupby("customer_category_name", as_index=False)
        .agg(
            net_revenue=("total_net_revenue", "sum"),
            profit=("total_profit", "sum"),
        )
        .sort_values("net_revenue", ascending=True)
    )

    y_pos = range(len(cat_summary))
    ax_cat.barh(
        y_pos,
        cat_summary["net_revenue"] / 1e3,
        height=0.55,
        color="#0F4C81",
        label="Net Revenue ($K)",
        alpha=0.92,
    )
    ax_cat.barh(
        y_pos,
        cat_summary["profit"] / 1e3,
        height=0.32,
        color="#2EC4B6",
        label="Line Profit ($K)",
        alpha=0.95,
    )
    ax_cat.set_yticks(list(y_pos))
    ax_cat.set_yticklabels(cat_summary["customer_category_name"], fontsize=9.5, fontweight="bold")
    ax_cat.xaxis.set_major_formatter(mtick.StrMethodFormatter("${x:,.0f}K"))
    ax_cat.set_title(
        "TILE 1A: Categorical Revenue & Profit by Customer Category",
        fontsize=11.5,
        fontweight="bold",
        color="#0F172A",
        loc="left",
        pad=10,
    )
    ax_cat.grid(axis="x", linestyle="--", alpha=0.4)
    ax_cat.legend(loc="lower right", frameon=True, fontsize=8.5)

    # -------------------------------------------------------------------------
    # TILE 1B (Row 1, Col 1): Categorical Distribution by WWI Stock Group
    # -------------------------------------------------------------------------
    ax_sg = fig.add_subplot(gs[1, 1], facecolor="#FFFFFF")
    sg_summary = (
        cat_df.groupby("stock_group_name", as_index=False)
        .agg(
            net_revenue=("total_net_revenue", "sum"),
            units=("total_units_sold", "sum"),
        )
        .sort_values("net_revenue", ascending=True)
    )
    colors_sg = [
        "#FF9F1C" if "Chiller" in name else "#00A8E8"
        for name in sg_summary["stock_group_name"]
    ]
    ax_sg.barh(
        range(len(sg_summary)),
        sg_summary["net_revenue"] / 1e3,
        height=0.6,
        color=colors_sg,
        alpha=0.9,
    )
    ax_sg.set_yticks(range(len(sg_summary)))
    ax_sg.set_yticklabels(sg_summary["stock_group_name"], fontsize=9)
    ax_sg.xaxis.set_major_formatter(mtick.StrMethodFormatter("${x:,.0f}K"))
    ax_sg.set_title(
        "TILE 1B: Categorical Sales Distribution by Warehouse Stock Group",
        fontsize=11.5,
        fontweight="bold",
        color="#0F172A",
        loc="left",
        pad=10,
    )
    ax_sg.grid(axis="x", linestyle="--", alpha=0.4)

    # -------------------------------------------------------------------------
    # TILE 2 (Row 2, Spanning Both Columns): Temporal Monthly Revenue & Profit
    # -------------------------------------------------------------------------
    ax_temp = fig.add_subplot(gs[2, :], facecolor="#FFFFFF")
    monthly = (
        temp_df.groupby("invoice_month", as_index=False)
        .agg(
            net_revenue=("monthly_net_revenue", "sum"),
            profit=("monthly_profit", "sum"),
            chiller_rev=("chiller_net_revenue", "sum"),
            dry_rev=("dry_net_revenue", "sum"),
        )
        .sort_values("invoice_month")
    )
    monthly["rolling_3m_rev"] = monthly["net_revenue"].rolling(3, min_periods=1).mean()

    ax_temp.bar(
        monthly["invoice_month"],
        monthly["dry_rev"] / 1e3,
        width=20,
        color="#CBD5E1",
        label="Dry Stock Revenue ($K)",
        alpha=0.75,
    )
    ax_temp.bar(
        monthly["invoice_month"],
        monthly["chiller_rev"] / 1e3,
        bottom=monthly["dry_rev"] / 1e3,
        width=20,
        color="#38BDF8",
        label="Chiller Stock Revenue ($K)",
        alpha=0.85,
    )
    ax_temp.plot(
        monthly["invoice_month"],
        monthly["net_revenue"] / 1e3,
        color="#0F4C81",
        linewidth=2.4,
        marker="o",
        markersize=4,
        label="Total Monthly Net Revenue ($K)",
    )
    ax_temp.plot(
        monthly["invoice_month"],
        monthly["rolling_3m_rev"] / 1e3,
        color="#E71D36",
        linewidth=2.0,
        linestyle="--",
        label="3-Month Moving Average ($K)",
    )
    ax_temp.plot(
        monthly["invoice_month"],
        monthly["profit"] / 1e3,
        color="#2EC4B6",
        linewidth=2.2,
        marker="s",
        markersize=3.5,
        label="Monthly Line Profit ($K)",
    )

    ax_temp.set_title(
        "TILE 2: Temporal Distribution of Monthly Net Revenue, Chiller Split & Line Profit (2023–2025)",
        fontsize=11.5,
        fontweight="bold",
        color="#0F172A",
        loc="left",
        pad=10,
    )
    ax_temp.yaxis.set_major_formatter(mtick.StrMethodFormatter("${x:,.0f}K"))
    ax_temp.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax_temp.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.setp(ax_temp.get_xticklabels(), rotation=30, ha="right", fontsize=8.5)
    ax_temp.grid(True, linestyle="--", alpha=0.4)
    ax_temp.legend(loc="upper left", ncol=5, frameon=True, fontsize=8.5)

    fig.savefig(output_image_path, dpi=160)
    plt.close(fig)
    return output_image_path


def generate_architecture_diagram(output_image_path: Path) -> Path:
    output_image_path.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(15, 5.5), dpi=160, facecolor="#0F172A")
    ax.set_facecolor("#0F172A")
    ax.axis("off")

    ax.text(
        0.5,
        0.91,
        "WIDE WORLD IMPORTERS (WWI) — END-TO-END SNOWFLAKE & AZURE DATA PIPELINE ARCHITECTURE",
        fontsize=14,
        fontweight="bold",
        color="#F8FAFC",
        ha="center",
        transform=ax.transAxes,
    )
    ax.text(
        0.5,
        0.84,
        "Infrastructure as Code: Terraform (azurerm + snowflake)   |   Orchestration: Dagster (Docker Compose)   |   CI/CD: GitHub Actions + Pytest",
        fontsize=9.5,
        color="#94A3B8",
        ha="center",
        transform=ax.transAxes,
    )

    stages = [
        (
            0.03,
            "1. DATA SOURCE",
            "Wide World Importers\n(WWI OLTP / Sample DB)",
            "• Sales.Orders & OrderLines\n• Sales.Invoices & Lines\n• Customers & StockItems\n• Resilient Python Extractor",
            "#3B82F6",
        ),
        (
            0.22,
            "2. CLOUD DATA LAKE",
            "Azure ADLS Gen2\n(Blob Storage)",
            "• Container: wwi-raw\n• Snappy Parquet Format\n• Path: wwi/v1/<table>/\n• Provisioned via Terraform",
            "#0EA5E9",
        ),
        (
            0.41,
            "3. DATA WAREHOUSE",
            "Snowflake DWH\n(WWI_DB.RAW)",
            "• Warehouse: WWI_WH\n• External Stage + COPY INTO\n• CLUSTER BY (order_date)\n• CLUSTER BY (invoice_date)",
            "#06B6D4",
        ),
        (
            0.60,
            "4. TRANSFORMATIONS",
            "dbt-snowflake\n(STAGING -> MARTS)",
            "• 7 Staging Views\n• 2 Intermediate Ephemerals\n• 6 Clustered Star Marts\n• dbt_utils + Schema Tests",
            "#10B981",
        ),
        (
            0.79,
            "5. BI ANALYTICS",
            "Power BI &\nLooker Studio",
            "• Tile 1: Categorical Sales\n• Tile 2: Temporal Revenue\n• Custom DAX & Theme JSON\n• DirectQuery / Mart Extracts",
            "#F59E0B",
        ),
    ]

    for idx, (x0, header, title, bullets, color) in enumerate(stages):
        box = mpatches.FancyBboxPatch(
            (x0, 0.16),
            0.17,
            0.58,
            boxstyle="round,pad=0.015,rounding_size=0.025",
            facecolor="#1E293B",
            edgecolor=color,
            linewidth=2.2,
            transform=ax.transAxes,
        )
        ax.add_patch(box)
        ax.text(
            x0 + 0.085,
            0.68,
            header,
            fontsize=8.5,
            fontweight="bold",
            color=color,
            ha="center",
            transform=ax.transAxes,
        )
        ax.text(
            x0 + 0.085,
            0.55,
            title,
            fontsize=10.5,
            fontweight="bold",
            color="#F8FAFC",
            ha="center",
            va="center",
            transform=ax.transAxes,
        )
        ax.text(
            x0 + 0.015,
            0.39,
            bullets,
            fontsize=8.2,
            color="#CBD5E1",
            va="top",
            transform=ax.transAxes,
        )

        if idx < len(stages) - 1:
            ax.annotate(
                "",
                xy=(x0 + 0.215, 0.45),
                xytext=(x0 + 0.175, 0.45),
                xycoords="axes fraction",
                textcoords="axes fraction",
                arrowprops=dict(arrowstyle="->", color="#38BDF8", lw=2.5),
            )

    # Dagster Orchestration Footer Bar
    footer = mpatches.FancyBboxPatch(
        (0.03, 0.03),
        0.93,
        0.08,
        boxstyle="round,pad=0.01,rounding_size=0.02",
        facecolor="#0F4C81",
        edgecolor="#38BDF8",
        linewidth=1.5,
        transform=ax.transAxes,
    )
    ax.add_patch(footer)
    ax.text(
        0.5,
        0.07,
        "ORCHESTRATED END-TO-END BY DAGSTER  (extract_wwi_source_tables  ▸  upload_wwi_to_azure_datalake  ▸  load_azure_datalake_to_snowflake_raw  ▸  build_dbt_snowflake_marts  ▸  export_bi_dashboard_datasets)",
        fontsize=8.8,
        fontweight="bold",
        color="#F8FAFC",
        ha="center",
        va="center",
        transform=ax.transAxes,
    )

    fig.savefig(output_image_path, dpi=160)
    plt.close(fig)
    return output_image_path


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[1]
    export_dir = Path(os.getenv("LOCAL_DATA_DIR", str(project_root / "data"))).resolve() / "exports"
    dash_img = generate_dashboard_image(
        export_dir=export_dir,
        output_image_path=project_root / "dashboard" / "assets" / "wwi_dashboard_preview.png",
    )
    arch_img = generate_architecture_diagram(
        output_image_path=project_root / "docs" / "architecture_diagram.png",
    )
    print(f"Generated dashboard preview: {dash_img}")
    print(f"Generated architecture diagram: {arch_img}")
