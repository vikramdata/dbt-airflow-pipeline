-- Singular test: the snapshot must have exactly one open (current) row per product.
select product_id, count(*) as current_rows
from {{ ref('products_snapshot') }}
where dbt_valid_to is null
group by 1
having count(*) <> 1
