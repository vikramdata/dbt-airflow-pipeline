-- Order lines joined to their order and product so marts don't repeat the joins.
select
    oi.order_item_id,
    oi.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    o.channel,
    oi.product_id,
    p.product_name,
    p.category,
    oi.quantity,
    oi.unit_price,
    oi.gross_amount,
    oi.discount_amount,
    oi.net_amount
from {{ ref('stg_order_items') }} oi
join {{ ref('stg_orders') }} o using (order_id)
join {{ ref('stg_products') }} p using (product_id)
