with source as (
    select * from {{ source('wwi_raw', 'raw_invoice_lines') }}
),

deduplicated as (
    select
        cast(invoice_line_id as integer) as invoice_line_id,
        cast(invoice_id as integer) as invoice_id,
        cast(stock_item_id as integer) as stock_item_id,
        trim(description) as line_description,
        cast(quantity as integer) as quantity,
        cast(unit_price as numeric(18, 2)) as unit_price,
        cast(tax_rate as numeric(18, 3)) as tax_rate,
        cast(tax_amount as numeric(18, 2)) as tax_amount,
        cast(line_profit as numeric(18, 2)) as line_profit,
        cast(extended_price as numeric(18, 2)) as extended_price,
        cast(ingested_at as timestamp_ntz) as ingested_at,
        row_number() over (partition by invoice_line_id order by ingested_at desc) as _rn
    from source
    where invoice_line_id is not null
)

select
    invoice_line_id,
    invoice_id,
    stock_item_id,
    line_description,
    quantity,
    unit_price,
    tax_rate,
    tax_amount,
    line_profit,
    extended_price,
    round(extended_price - tax_amount, 2) as net_revenue,
    ingested_at
from deduplicated
where _rn = 1
