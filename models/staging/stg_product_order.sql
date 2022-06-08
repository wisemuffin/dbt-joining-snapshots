with product_order as (

    select 
        *

    from {{ ref('product_order_snapshot') }}

)

, data_types_and_renaming as (
    select
        PRODUCT_ID,
        ORDER_ID,
        PRODUCT_ORDER_ID,
        ORDER_STATUS,
        UPDATED_AT,
        DBT_SCD_ID,
        DBT_UPDATED_AT,
        DBT_VALID_FROM,
        DBT_VALID_TO
    from product_order
)

, grain_id as (

    select 
        {{ build_key_from_columns(table_name=ref('product_order_snapshot'), exclude=['UPDATED_AT','DBT_SCD_ID', 'DBT_UPDATED_AT', 'DBT_VALID_FROM', 'DBT_VALID_TO']) }} as grain_id,
        *

    from data_types_and_renaming

) 

, mark_real_diffs as (

  select

      *,
      coalesce(
          lag(grain_id) over (partition by product_order_id order by updated_at),
          'first_record'
      ) as previous_grain_id,
      case
          when grain_id != previous_grain_id then true 
          else false
      end as is_real_diff

  from grain_id

),

filter_real_diffs as (

    select *
  
    from mark_real_diffs
  
    where is_real_diff = true

)

select * from filter_real_diffs