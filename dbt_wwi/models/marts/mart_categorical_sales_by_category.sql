{{
    config(
        materialized='table',
        cluster_by=['customer_category_name', 'stock_group_name']
    )
}}

with invoices as (
    select * from {{ ref('fct_invoices') }}
)

select
    customer_category_name,
    stock_group_name,
    sales_territory,
    buying_group_name,
    count(distinct invoice_id) as total_invoices,
    count(distinct customer_id) as active_customers,
    sum(quantity) as total_units_sold,
    round(sum(net_revenue), 2) as total_net_revenue,
    round(sum(tax_amount), 2) as total_tax_amount,
    round(sum(extended_price), 2) as total_gross_revenue,
    round(sum(line_profit), 2) as total_profit,
    round(
        case when sum(net_revenue) > 0
            then (sum(line_profit) * 100.0) / sum(net_revenue)
            else 0
        end,
        2
    ) as profit_margin_pct,
    round(
        case when count(distinct invoice_id) > 0
            then sum(net_revenue) / count(distinct invoice_id)
            else 0
        end,
        2
    ) as avg_invoice_revenue
from invoices
group by
    customer_category_name,
    stock_group_name,
    sales_territory,
    buying_group_name
