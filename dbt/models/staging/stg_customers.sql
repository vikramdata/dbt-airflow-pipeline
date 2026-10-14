with source as (
    select * from {{ source('raw', 'customers') }}
)

select
    customer_id,
    initcap(first_name)          as first_name,
    initcap(last_name)           as last_name,
    lower(trim(email))           as email,
    city,
    country,
    signup_date,
    lower(loyalty_tier)          as loyalty_tier,
    updated_at
from source
