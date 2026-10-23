with source as (
    select * from {{ source('wwi_raw', 'raw_order_lines') }}
),

deduplicated as (
    select
        cast(order_line_id as integer) as order_line_id,
        cast(order_id as integer) as order_id,
        cast(stock_item_id as integer) as stock_item_id,
        trim(description) as line_description,
        cast(quantity as integer) as quantity,
        cast(unit_price as numeric(18, 2)) as unit_price,
        cast(tax_rate as numeric(18, 3)) as tax_rate,
        cast(picked_quantity as integer) as picked_quantity,
        cast(ingested_at as timestamp_ntz) as ingested_at,
        row_number() over (partition by order_line_id order by ingested_at desc) as _rn
    from source
    where order_line_id is not null
)

select
    order_line_id,
    order_id,
    stock_item_id,
    line_description,
    quantity,
    unit_price,
    tax_rate,
    picked_quantity,
    round(quantity * unit_price, 2) as net_order_amount,
    round(quantity * unit_price * (tax_rate / 100.0), 2) as estimated_tax_amount,
    ingested_at
from deduplicated
where _rn = 1
