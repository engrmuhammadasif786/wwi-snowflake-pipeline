with source as (
    select * from {{ source('wwi_raw', 'raw_customers') }}
),

deduplicated as (
    select
        cast(customer_id as integer) as customer_id,
        trim(customer_name) as customer_name,
        cast(customer_category_id as integer) as customer_category_id,
        trim(customer_category_name) as customer_category_name,
        coalesce(nullif(trim(buying_group_name), ''), 'Independent') as buying_group_name,
        cast(delivery_city_id as integer) as delivery_city_id,
        trim(delivery_city_name) as delivery_city_name,
        upper(trim(state_province_code)) as state_province_code,
        trim(state_province_name) as state_province_name,
        trim(sales_territory) as sales_territory,
        cast(credit_limit as numeric(18, 2)) as credit_limit,
        cast(account_opened_date as date) as account_opened_date,
        cast(standard_discount_percentage as numeric(18, 3)) as standard_discount_percentage,
        coalesce(cast(is_on_credit_hold as boolean), false) as is_on_credit_hold,
        cast(ingested_at as timestamp_ntz) as ingested_at,
        row_number() over (partition by customer_id order by ingested_at desc) as _rn
    from source
    where customer_id is not null
)

select
    customer_id,
    customer_name,
    customer_category_id,
    customer_category_name,
    buying_group_name,
    delivery_city_id,
    delivery_city_name,
    state_province_code,
    state_province_name,
    sales_territory,
    credit_limit,
    account_opened_date,
    standard_discount_percentage,
    is_on_credit_hold,
    ingested_at
from deduplicated
where _rn = 1
