with customers as (
    select * from {{ ref('stg_customers') }}
),

order_stats as (
    select
        customer_id,
        min(order_date)                                       as first_order_date,
        max(order_date)                                       as last_order_date,
        count(*) filter (where is_completed)                  as completed_orders,
        coalesce(sum(net_revenue) filter (where is_completed), 0) as lifetime_revenue
    from {{ ref('fct_orders') }}
    group by 1
),

tier_changes as (
    select customer_id, count(*) - 1 as tier_changes
    from {{ ref('customers_snapshot') }}
    group by 1
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.city,
    c.country,
    c.signup_date,
    c.loyalty_tier,
    coalesce(t.tier_changes, 0)            as tier_changes,
    s.first_order_date,
    s.last_order_date,
    coalesce(s.completed_orders, 0)        as completed_orders,
    coalesce(s.lifetime_revenue, 0)        as lifetime_revenue,
    case
        when s.completed_orders is null     then 'never_ordered'
        when s.lifetime_revenue >= 5000    then 'high_value'
        when s.completed_orders >= 2        then 'repeat'
        else 'one_time'
    end                                    as customer_segment
from customers c
left join order_stats s using (customer_id)
left join tier_changes t using (customer_id)
