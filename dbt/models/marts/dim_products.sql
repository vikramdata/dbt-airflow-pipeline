with history as (
    select * from {{ ref('products_snapshot') }}
),

current_version as (
    select * from history where dbt_valid_to is null
),

price_stats as (
    select
        product_id,
        count(*) - 1                                    as price_changes,
        {{ cents_to_dollars('min(unit_price_cents)') }} as min_price,
        {{ cents_to_dollars('max(unit_price_cents)') }} as max_price
    from history
    group by 1
)

select
    c.product_id,
    c.product_name,
    c.category,
    {{ cents_to_dollars('c.unit_price_cents') }} as current_price,
    s.min_price,
    s.max_price,
    s.price_changes,
    c.dbt_valid_from                             as price_effective_from
from current_version c
join price_stats s using (product_id)
