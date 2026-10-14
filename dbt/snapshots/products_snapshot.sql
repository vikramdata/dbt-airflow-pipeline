{#
  SCD Type 2 history of product prices.
  Every time a product's updated_at changes, dbt closes the old row
  (dbt_valid_to) and inserts a new one, so we keep the full price history.
#}
{% snapshot products_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='product_id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('raw', 'products') }}

{% endsnapshot %}
