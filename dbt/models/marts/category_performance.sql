-- Weekly revenue and units by product category (completed orders only).
select
    date_trunc('week', order_date)::date   as week_start,
    category,
    count(distinct order_id)               as orders,
    sum(quantity)                          as units,
    sum(net_amount)                        as net_revenue,
    round(avg(net_amount), 2)              as avg_line_value
from {{ ref('int_order_items_enriched') }}
where status = 'completed'
group by 1, 2
