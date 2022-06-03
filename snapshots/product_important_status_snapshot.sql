{% snapshot product_important_status_snapshot %}

{{
    config(
      target_database='DBT_JOINING_SNAPSHOTS_DATABASE',
      target_schema='snapshots',
      unique_key='entity_id',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ ref('product_important_status') }}

{% endsnapshot %}