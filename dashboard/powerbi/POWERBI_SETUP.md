# Power BI Setup Guide — Wide World Importers (WWI) Snowflake Analytics

This guide explains how to connect **Power BI Desktop** either directly to **Snowflake (`WWI_DB.MARTS`)** or to the pre-exported CSV/Parquet marts in `data/exports/` for offline peer review.

---

## Option A: Connect Directly to Snowflake (`WWI_DB.MARTS`)

1. Open **Power BI Desktop** -> **Get Data** -> **Database** -> **Snowflake**.
2. Enter your Snowflake configuration:
   - **Server**: `<SNOWFLAKE_ACCOUNT>.snowflakecomputing.com`
   - **Warehouse**: `WWI_WH`
   - **Data Connectivity Mode**: Select **DirectQuery** (leverages Snowflake micro-partition clustering) or **Import**.
3. Authenticate using your `SNOWFLAKE_USER` and `SNOWFLAKE_PASSWORD`.
4. In the Navigator, expand **`WWI_DB` -> `MARTS`** and select:
   - `MART_CATEGORICAL_SALES_BY_CATEGORY` *(Tile 1 — Categorical Distribution)*
   - `MART_TEMPORAL_MONTHLY_REVENUE` *(Tile 2 — Temporal Distribution)*
   - `DIM_CUSTOMERS`, `DIM_STOCK_ITEMS`, `FCT_INVOICES` *(Optional drill-through star schema)*

---

## Option B: Offline / Peer-Review Mode (`data/exports/`)

1. Run `make pipeline-run` to materialize all dbt marts locally into `data/exports/`.
2. In **Power BI Desktop**, click **Get Data** -> **Text/CSV** (or **Parquet**) and load:
   - `data/exports/mart_categorical_sales_by_category.csv`
   - `data/exports/mart_temporal_monthly_revenue.csv`

---

## Applying the Theme and DAX Measures

1. In Power BI Desktop, go to **View** -> **Themes** -> **Browse for themes** and select `dashboard/powerbi/wwi_snowflake_theme.json`.
2. Add the DAX measures from `dashboard/powerbi/wwi_dax_measures.dax`.
3. Configure the two core dashboard tiles:
   - **Tile 1 (Categorical Distribution)**: Clustered Bar / Stacked Column chart showing **Total Net Revenue ($)** and **Profit Margin (%)** by `customer_category_name` and `stock_group_name`.
   - **Tile 2 (Temporal Distribution)**: Line and Clustered Column chart showing **Monthly Net Revenue ($)**, **Monthly Profit ($)**, and **3-Month Rolling Trend** across `invoice_month` (2023–2025).
