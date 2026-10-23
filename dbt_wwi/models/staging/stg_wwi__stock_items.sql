with source as (
    select * from {{ source('wwi_raw', 'raw_stock_items') }}
),

deduplicated as (
    select
        cast(stock_item_id as integer) as stock_item_id,
        trim(stock_item_name) as stock_item_name,
        cast(supplier_id as integer) as supplier_id,
        cast(stock_group_id as integer) as stock_group_id,
        trim(stock_group_name) as stock_group_name,
        coalesce(nullif(trim(brand), ''), 'WWI Generic') as brand,
        coalesce(nullif(trim(size), ''), 'Standard') as item_size,
        cast(lead_time_days as integer) as lead_time_days,
        coalesce(cast(is_chiller_stock as boolean), false) as is_chiller_stock,
        cast(tax_rate as numeric(18, 3)) as tax_rate,
        cast(unit_price as numeric(18, 2)) as catalog_unit_price,
        cast(recommended_retail_price as numeric(18, 2)) as recommended_retail_price,
        cast(typical_weight_per_unit as numeric(18, 3)) as typical_weight_per_unit,
        cast(ingested_at as timestamp_ntz) as ingested_at,
        row_number() over (partition by stock_item_id order by ingested_at desc) as _rn
    from source
    where stock_item_id is not null
)

select
    stock_item_id,
    stock_item_name,
    supplier_id,
    stock_group_id,
    stock_group_name,
    brand,
    item_size,
    lead_time_days,
    is_chiller_stock,
    tax_rate,
    catalog_unit_price,
    recommended_retail_price,
    typical_weight_per_unit,
    ingested_at
from deduplicated
where _rn = 1
