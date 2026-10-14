-- Singular test: revenue on each order must equal the sum of its line items.
-- Returns rows (= failure) for any order that is off by more than a cent.
with lines as (
    select order_id, sum(net_amount) as line_total
    from {{ ref('stg_order_items') }}
    group by 1
)

select o.order_id, o.net_revenue, l.line_total
from {{ ref('fct_orders') }} o
join lines l using (order_id)
where abs(o.net_revenue - l.line_total) > 0.01
