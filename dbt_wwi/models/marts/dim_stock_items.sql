{{
    config(
        materialized='table',
        cluster_by=['stock_group_name']
    )
}}

with stock_items as (
    select * from {{ ref('stg_wwi__stock_items') }}
),

suppliers as (
    select * from {{ ref('stg_wwi__suppliers') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['si.stock_item_id']) }} as stock_item_sk,
    si.stock_item_id,
    si.stock_item_name,
    si.stock_group_id,
    si.stock_group_name,
    si.brand,
    si.item_size,
    si.lead_time_days,
    si.is_chiller_stock,
    case when si.is_chiller_stock then 'Chiller Stock' else 'Dry Stock' end as storage_type,
    si.tax_rate,
    si.catalog_unit_price,
    si.recommended_retail_price,
    round(si.recommended_retail_price - si.catalog_unit_price, 2) as catalog_markup_amount,
    si.typical_weight_per_unit,
    s.supplier_id,
    s.supplier_name,
    s.supplier_category_name,
    s.payment_days as supplier_payment_days
from stock_items si
left join suppliers s
    on si.supplier_id = s.supplier_id
