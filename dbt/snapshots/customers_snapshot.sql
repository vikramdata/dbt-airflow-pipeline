{#
  SCD Type 2 history of customer loyalty tiers.
  Uses the 'check' strategy: a new version is recorded whenever loyalty_tier changes.
#}
{% snapshot customers_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='check',
      check_cols=['loyalty_tier'],
    )
}}

select * from {{ source('raw', 'customers') }}

{% endsnapshot %}
