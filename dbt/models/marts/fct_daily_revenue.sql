with orders as (
    select * from {{ ref('fct_orders') }}
),

new_customers as (
    select signup_date as order_date, count(*) as new_customers
    from {{ ref('stg_customers') }}
    group by 1
)

select
    o.order_date,
    count(*)                                                     as total_orders,
    count(*) filter (where o.is_completed)                       as completed_orders,
    count(*) filter (where o.status = 'returned')                as returned_orders,
    count(*) filter (where o.status = 'cancelled')               as cancelled_orders,
    sum(o.net_revenue) filter (where o.is_completed)             as net_revenue,
    round(avg(o.net_revenue) filter (where o.is_completed), 2)   as avg_order_value,
    coalesce(max(n.new_customers), 0)                            as new_customers
from orders o
left join new_customers n using (order_date)
group by 1
