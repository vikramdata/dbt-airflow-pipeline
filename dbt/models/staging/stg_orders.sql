with source as (
    select * from {{ source('raw', 'orders') }}
)

select
    order_id,
    customer_id,
    order_ts,
    order_ts::date                     as order_date,
    lower(trim(status))                as status,
    coalesce(channel, 'unknown')       as channel,
    _loaded_at                         as loaded_at
from source
