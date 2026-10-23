-- =============================================================================
-- Dashboard Tile 2: Temporal Distribution of Monthly Revenue, Profit & Volume
-- Source Mart: WWI_DB.MARTS.MART_TEMPORAL_MONTHLY_REVENUE
-- Clustered By: (INVOICE_MONTH, SALES_TERRITORY)
-- Used by: Power BI & Looker Studio
-- =============================================================================

SELECT
    INVOICE_MONTH,
    INVOICE_YEAR,
    INVOICE_QUARTER,
    SALES_TERRITORY,
    CUSTOMER_CATEGORY_NAME,
    TOTAL_INVOICES,
    TOTAL_ORDERS,
    ACTIVE_CUSTOMERS,
    TOTAL_UNITS_SOLD,
    MONTHLY_NET_REVENUE,
    MONTHLY_GROSS_REVENUE,
    MONTHLY_PROFIT,
    PROFIT_MARGIN_PCT,
    CHILLER_NET_REVENUE,
    DRY_NET_REVENUE,
    PREV_MONTH_NET_REVENUE,
    YTD_NET_REVENUE
FROM WWI_DB.MARTS.MART_TEMPORAL_MONTHLY_REVENUE
ORDER BY
    INVOICE_MONTH ASC,
    SALES_TERRITORY ASC;
