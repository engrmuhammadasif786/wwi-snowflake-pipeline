with source as (
    select * from {{ source('wwi_raw', 'raw_orders') }}
),

deduplicated as (
    select
        cast(order_id as integer) as order_id,
        cast(customer_id as integer) as customer_id,
        cast(salesperson_person_id as integer) as salesperson_person_id,
        cast(order_date as date) as order_date,
        cast(expected_delivery_date as date) as expected_delivery_date,
        trim(customer_purchase_order_number) as customer_purchase_order_number,
        coalesce(cast(is_undersupply_backordered as boolean), false) as is_undersupply_backordered,
        cast(ingested_at as timestamp_ntz) as ingested_at,
        row_number() over (partition by order_id order by ingested_at desc) as _rn
    from source
    where order_id is not null
)

select
    order_id,
    customer_id,
    salesperson_person_id,
    order_date,
    expected_delivery_date,
    datediff('day', order_date, expected_delivery_date) as promised_lead_time_days,
    customer_purchase_order_number,
    is_undersupply_backordered,
    ingested_at
from deduplicated
where _rn = 1
