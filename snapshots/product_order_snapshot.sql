with product_order as (
  select 
    PRODUCT_ID,
    ORDER_ID,
    PRODUCT_ORDER_ID,
    ORDER_STATUS,
    UPDATED_AT
  from {{ ref('product_order') }}
)

{% snapshot product_order_snapshot %}

{{
    config(
      target_database='DBT_JOINING_SNAPSHOTS_DATABASE',
      target_schema='snapshots',
      unique_key='product_order_id',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from product_order

{% endsnapshot %}