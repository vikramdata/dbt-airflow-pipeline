with source as (
    select * from {{ source('raw', 'products') }}
)

select
    product_id,
    product_name,
    category,
    unit_price_cents,
    {{ cents_to_dollars('unit_price_cents') }} as unit_price,
    updated_at
from source
