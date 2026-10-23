with orders as (
    select * from {{ ref('stg_wwi__orders') }}
),

order_lines as (
    select * from {{ ref('stg_wwi__order_lines') }}
),

customers as (
    select * from {{ ref('stg_wwi__customers') }}
),

stock_items as (
    select * from {{ ref('stg_wwi__stock_items') }}
)

select
    ol.order_line_id,
    o.order_id,
    o.order_date,
    date_trunc('month', o.order_date) as order_month,
    o.expected_delivery_date,
    o.promised_lead_time_days,
    o.is_undersupply_backordered,
    c.customer_id,
    c.customer_name,
    c.customer_category_id,
    c.customer_category_name,
    c.buying_group_name,
    c.sales_territory,
    c.state_province_code,
    si.stock_item_id,
    si.stock_item_name,
    si.stock_group_id,
    si.stock_group_name,
    si.is_chiller_stock,
    ol.quantity,
    ol.picked_quantity,
    ol.unit_price,
    ol.tax_rate,
    ol.net_order_amount,
    ol.estimated_tax_amount,
    round(ol.net_order_amount + ol.estimated_tax_amount, 2) as gross_order_amount
from order_lines ol
inner join orders o
    on ol.order_id = o.order_id
inner join customers c
    on o.customer_id = c.customer_id
inner join stock_items si
    on ol.stock_item_id = si.stock_item_id
