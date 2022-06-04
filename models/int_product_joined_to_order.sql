with historical_table_1 as (
    select *,
        dbt_valid_from as valid_from,
        coalesce(dbt_valid_to, cast('{{ var("high_date") }}' as timestamp)) as valid_to
    from {{ ref('stg_product_important_status') }}
)

, historical_table_2 as (
    select *,
        dbt_valid_from as valid_from,
        coalesce(dbt_valid_to, cast('{{ var("high_date") }}' as timestamp)) as valid_to
    from {{ ref('stg_product_order') }}
)

, build_spine as (

    select
        historical_table_1.*,
        historical_table_2.product_order_id
    from historical_table_1
    left join
        historical_table_2 
        on historical_table_1.product_id = historical_table_2.product_id
)

, snap_join as  (
    select
    -- requires any extra columns from table_join_on to be listed prior to using this macro.
    -- assumes we have replaced instances of valid_to = null with a future_proof_date = '9999-12-31'.
    {{ join_snapshots(
            cte_join='build_spine', 
            cte_join_on='historical_table_2', 
            cte_join_valid_to='valid_to',
            cte_join_valid_from='valid_from', 
            cte_join_on_valid_to='valid_to', 
            cte_join_on_valid_from='valid_from',
            cte_join_id='product_id', 
            cte_join_on_id='product_id'
    ) }}
)
select *
from snap_join