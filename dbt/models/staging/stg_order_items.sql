with source as (
    select * from {{ source('raw', 'order_items') }}
)

select
    order_item_id,
    order_id,
    product_id,
    quantity,
    {{ cents_to_dollars('unit_price_cents') }}                                  as unit_price,
    discount_pct,
    {{ cents_to_dollars('quantity * unit_price_cents') }}                       as gross_amount,
    {{ cents_to_dollars('quantity * unit_price_cents * discount_pct') }}        as discount_amount,
    {{ cents_to_dollars('quantity * unit_price_cents * (1 - discount_pct)') }}  as net_amount
from source
