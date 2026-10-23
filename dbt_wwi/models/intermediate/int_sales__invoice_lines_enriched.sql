with invoices as (
    select * from {{ ref('stg_wwi__invoices') }}
),

invoice_lines as (
    select * from {{ ref('stg_wwi__invoice_lines') }}
),

customers as (
    select * from {{ ref('stg_wwi__customers') }}
),

stock_items as (
    select * from {{ ref('stg_wwi__stock_items') }}
),

suppliers as (
    select * from {{ ref('stg_wwi__suppliers') }}
)

select
    il.invoice_line_id,
    i.invoice_id,
    i.order_id,
    i.invoice_date,
    i.invoice_month,
    i.invoice_year,
    i.invoice_quarter,
    i.is_credit_note,
    c.customer_id,
    c.customer_name,
    c.customer_category_id,
    c.customer_category_name,
    c.buying_group_name,
    c.delivery_city_name,
    c.state_province_code,
    c.state_province_name,
    c.sales_territory,
    si.stock_item_id,
    si.stock_item_name,
    si.stock_group_id,
    si.stock_group_name,
    si.brand,
    si.is_chiller_stock,
    s.supplier_id,
    s.supplier_name,
    s.supplier_category_name,
    il.quantity,
    il.unit_price,
    il.tax_rate,
    il.tax_amount,
    il.net_revenue,
    il.line_profit,
    il.extended_price
from invoice_lines il
inner join invoices i
    on il.invoice_id = i.invoice_id
inner join customers c
    on i.customer_id = c.customer_id
inner join stock_items si
    on il.stock_item_id = si.stock_item_id
left join suppliers s
    on si.supplier_id = s.supplier_id
