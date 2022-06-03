with product_important_status as (

    select 
        *

    from {{ ref('product_important_status_snapshot') }}

)

, grain_id as (

    select 
        {{ build_key_from_columns(table_name=ref('product_important_status_snapshot'), columns=['entity_id','important_status']) }} as grain_id,
        *

    from product_important_status

) 

, mark_real_diffs as (

  select

      *,
      coalesce(
          lag(grain_id) over (partition by entity_id order by updated_at),
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