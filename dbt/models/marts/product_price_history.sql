-- Every price a product has had, with the date range it was valid for.
select
    product_id,
    product_name,
    category,
    {{ cents_to_dollars('unit_price_cents') }}  as unit_price,
    dbt_valid_from                              as valid_from,
    dbt_valid_to                                as valid_to,
    dbt_valid_to is null                        as is_current
from {{ ref('products_snapshot') }}
