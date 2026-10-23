{{
    config(
        materialized='table',
        cluster_by=['invoice_month', 'sales_territory']
    )
}}

with monthly_agg as (
    select
        invoice_month,
        invoice_year,
        invoice_quarter,
        sales_territory,
        customer_category_name,
        count(distinct invoice_id) as total_invoices,
        count(distinct order_id) as total_orders,
        count(distinct customer_id) as active_customers,
        sum(quantity) as total_units_sold,
        round(sum(net_revenue), 2) as monthly_net_revenue,
        round(sum(extended_price), 2) as monthly_gross_revenue,
        round(sum(line_profit), 2) as monthly_profit,
        round(sum(case when is_chiller_stock then net_revenue else 0 end), 2) as chiller_net_revenue,
        round(sum(case when not is_chiller_stock then net_revenue else 0 end), 2) as dry_net_revenue
    from {{ ref('fct_invoices') }}
    group by
        invoice_month,
        invoice_year,
        invoice_quarter,
        sales_territory,
        customer_category_name
)

select
    invoice_month,
    invoice_year,
    invoice_quarter,
    sales_territory,
    customer_category_name,
    total_invoices,
    total_orders,
    active_customers,
    total_units_sold,
    monthly_net_revenue,
    monthly_gross_revenue,
    monthly_profit,
    round(
        case when monthly_net_revenue > 0
            then (monthly_profit * 100.0) / monthly_net_revenue
            else 0
        end,
        2
    ) as profit_margin_pct,
    chiller_net_revenue,
    dry_net_revenue,
    lag(monthly_net_revenue) over (
        partition by sales_territory, customer_category_name
        order by invoice_month
    ) as prev_month_net_revenue,
    round(
        sum(monthly_net_revenue) over (
            partition by sales_territory, customer_category_name, invoice_year
            order by invoice_month
            rows between unbounded preceding and current row
        ),
        2
    ) as ytd_net_revenue
from monthly_agg
