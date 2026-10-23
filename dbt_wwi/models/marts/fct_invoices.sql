{{
    config(
        materialized='table',
        cluster_by=['to_date(invoice_date)', 'customer_category_name', 'stock_group_name']
    )
}}

with enriched_invoices as (
    select * from {{ ref('int_sales__invoice_lines_enriched') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['invoice_line_id']) }} as invoice_line_sk,
    invoice_line_id,
    invoice_id,
    order_id,
    invoice_date,
    invoice_month,
    invoice_year,
    invoice_quarter,
    is_credit_note,
    customer_id,
    customer_name,
    customer_category_id,
    customer_category_name,
    buying_group_name,
    delivery_city_name,
    state_province_code,
    state_province_name,
    sales_territory,
    stock_item_id,
    stock_item_name,
    stock_group_id,
    stock_group_name,
    brand,
    is_chiller_stock,
    supplier_id,
    supplier_name,
    supplier_category_name,
    quantity,
    unit_price,
    tax_rate,
    tax_amount,
    net_revenue,
    line_profit,
    extended_price,
    round(case when net_revenue > 0 then (line_profit * 100.0) / net_revenue else 0 end, 2) as profit_margin_pct
from enriched_invoices
