{{
    config(
        materialized='table',
        cluster_by=['to_date(order_date)', 'customer_category_name']
    )
}}

with enriched_orders as (
    select * from {{ ref('int_sales__order_lines_enriched') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['order_line_id']) }} as order_line_sk,
    order_line_id,
    order_id,
    order_date,
    order_month,
    expected_delivery_date,
    promised_lead_time_days,
    is_undersupply_backordered,
    customer_id,
    customer_name,
    customer_category_id,
    customer_category_name,
    buying_group_name,
    sales_territory,
    state_province_code,
    stock_item_id,
    stock_item_name,
    stock_group_id,
    stock_group_name,
    is_chiller_stock,
    quantity,
    picked_quantity,
    round(case when quantity > 0 then (picked_quantity * 100.0) / quantity else 0 end, 2) as fulfillment_rate_pct,
    unit_price,
    tax_rate,
    net_order_amount,
    estimated_tax_amount,
    gross_order_amount
from enriched_orders
