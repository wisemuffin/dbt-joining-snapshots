with product_important_status as (
  select 
    PRODUCT_ID,
    UNIMPORTANT_VALUE,
    IMPORTANT_STATUS,
    UPDATED_AT
  from {{ ref('product_important_status') }}
)

{% snapshot product_important_status_snapshot %}

{{
    config(
      target_database='DBT_JOINING_SNAPSHOTS_DATABASE',
      target_schema='snapshots',
      unique_key='product_id',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from product_important_status

{% endsnapshot %}