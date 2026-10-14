{#
  Incremental fact table: only the last few days are reprocessed on each run,
  which handles late-arriving changes without rebuilding the whole table.
#}
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
    )
}}

with items as (
    select * from {{ ref('int_order_items_enriched') }}
    {% if is_incremental() %}
    where order_date >= (select max(order_date) - interval '3 days' from {{ this }})
    {% endif %}
)

select
    order_id,
    customer_id,
    order_date,
    status,
    channel,
    count(*)                         as item_count,
    sum(quantity)                    as units,
    sum(gross_amount)                as gross_revenue,
    sum(discount_amount)             as discount_amount,
    sum(net_amount)                  as net_revenue,
    status = 'completed'             as is_completed
from items
group by 1, 2, 3, 4, 5
