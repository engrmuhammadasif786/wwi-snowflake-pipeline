{{
    config(
        materialized='table',
        cluster_by=['sales_territory', 'customer_category_name']
    )
}}

with customers as (
    select * from {{ ref('stg_wwi__customers') }}
),

invoice_metrics as (
    select
        customer_id,
        count(distinct invoice_id) as lifetime_invoice_count,
        sum(net_revenue) as lifetime_net_revenue,
        sum(line_profit) as lifetime_profit,
        min(invoice_date) as first_invoice_date,
        max(invoice_date) as last_invoice_date
    from {{ ref('int_sales__invoice_lines_enriched') }}
    group by customer_id
)

select
    {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} as customer_sk,
    c.customer_id,
    c.customer_name,
    c.customer_category_id,
    c.customer_category_name,
    c.buying_group_name,
    c.delivery_city_id,
    c.delivery_city_name,
    c.state_province_code,
    c.state_province_name,
    c.sales_territory,
    c.credit_limit,
    c.account_opened_date,
    c.standard_discount_percentage,
    c.is_on_credit_hold,
    coalesce(im.lifetime_invoice_count, 0) as lifetime_invoice_count,
    coalesce(im.lifetime_net_revenue, 0.00) as lifetime_net_revenue,
    coalesce(im.lifetime_profit, 0.00) as lifetime_profit,
    im.first_invoice_date,
    im.last_invoice_date
from customers c
left join invoice_metrics im
    on c.customer_id = im.customer_id
