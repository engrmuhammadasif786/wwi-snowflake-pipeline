with source as (
    select * from {{ source('wwi_raw', 'raw_suppliers') }}
),

deduplicated as (
    select
        cast(supplier_id as integer) as supplier_id,
        trim(supplier_name) as supplier_name,
        trim(supplier_category_name) as supplier_category_name,
        trim(delivery_city_name) as delivery_city_name,
        upper(trim(state_province_code)) as state_province_code,
        cast(payment_days as integer) as payment_days,
        cast(ingested_at as timestamp_ntz) as ingested_at,
        row_number() over (partition by supplier_id order by ingested_at desc) as _rn
    from source
    where supplier_id is not null
)

select
    supplier_id,
    supplier_name,
    supplier_category_name,
    delivery_city_name,
    state_province_code,
    payment_days,
    ingested_at
from deduplicated
where _rn = 1
