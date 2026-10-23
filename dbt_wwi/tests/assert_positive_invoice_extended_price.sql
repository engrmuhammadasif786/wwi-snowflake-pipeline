-- Singular dbt data quality test:
-- Ensures no invoice line in fct_invoices has a negative or zero extended_price
select
    invoice_line_id,
    invoice_id,
    extended_price
from {{ ref('fct_invoices') }}
where extended_price <= 0
