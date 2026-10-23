with source as (
    select * from {{ source('wwi_raw', 'raw_invoices') }}
),

deduplicated as (
    select
        cast(invoice_id as integer) as invoice_id,
        cast(customer_id as integer) as customer_id,
        cast(order_id as integer) as order_id,
        cast(delivery_method_id as integer) as delivery_method_id,
        cast(invoice_date as date) as invoice_date,
        coalesce(cast(is_credit_note as boolean), false) as is_credit_note,
        coalesce(cast(total_dry_items as integer), 0) as total_dry_items,
        coalesce(cast(total_chiller_items as integer), 0) as total_chiller_items,
        cast(ingested_at as timestamp_ntz) as ingested_at,
        row_number() over (partition by invoice_id order by ingested_at desc) as _rn
    from source
    where invoice_id is not null
)

select
    invoice_id,
    customer_id,
    order_id,
    delivery_method_id,
    invoice_date,
    date_trunc('month', invoice_date) as invoice_month,
    extract(year from invoice_date) as invoice_year,
    extract(quarter from invoice_date) as invoice_quarter,
    is_credit_note,
    total_dry_items,
    total_chiller_items,
    ingested_at
from deduplicated
where _rn = 1
