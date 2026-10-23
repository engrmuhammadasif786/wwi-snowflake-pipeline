# Looker Studio Setup Guide — Wide World Importers (WWI) Snowflake Analytics

This guide explains how to build and view the two-tile WWI Executive Dashboard in **Google Looker Studio** using either the Snowflake Partner Connector or the exported CSV marts.

---

## 1. Connect Data Source

### Option A: Snowflake Connector
1. Open [Looker Studio](https://lookerstudio.google.com/) -> **Create** -> **Data Source**.
2. Search for **Snowflake** and authenticate with your Snowflake Account URL, `WWI_WH`, database `WWI_DB`, and schema `MARTS`.
3. Add two data sources using the custom SQL queries in:
   - `dashboard/sql/tile_1_categorical_sales_query.sql`
   - `dashboard/sql/tile_2_temporal_revenue_query.sql`

### Option B: File Upload Connector (Zero-Cost Peer Review Mode)
1. Run `make pipeline-run` to generate the CSV marts in `data/exports/`.
2. In Looker Studio -> **Create** -> **Data Source** -> **File Upload**.
3. Upload:
   - `data/exports/mart_categorical_sales_by_category.csv`
   - `data/exports/mart_temporal_monthly_revenue.csv`

---

## 2. Configure the Required Dashboard Tiles

### Tile 1: Categorical Distribution (`MART_CATEGORICAL_SALES_BY_CATEGORY`)
- **Chart Type**: Horizontal Bar Chart + Stacked Breakdown by Stock Group
- **Dimension**: `customer_category_name` (Breakdown Dimension: `stock_group_name`)
- **Metric**: `total_net_revenue` (Currency USD) & `total_profit` (Currency USD)
- **Sort**: `total_net_revenue` Descending

### Tile 2: Temporal Distribution (`MART_TEMPORAL_MONTHLY_REVENUE`)
- **Chart Type**: Time Series / Combo Chart (Bars + Line)
- **Dimension**: `invoice_month` (Year-Month)
- **Metrics**:
  - `monthly_net_revenue` (Line, Left Axis, USD)
  - `monthly_profit` (Filled Area / Bar, Left Axis, USD)
  - `profit_margin_pct` (Line, Right Axis, %)
