with product_important_status as (
    select *,
        dbt_valid_from as valid_from_pis,
        coalesce(dbt_valid_to, cast('{{ var("high_date") }}' as timestamp)) as valid_to_pis
    from {{ ref('stg_product_important_status') }}

    {% if var("current_records_only") %}

    where valid_to = cast('{{ var("high_date") }}' as timestamp)

    {% endif %}
)

, product_order as (
    select *,
        dbt_valid_from as valid_from_po,
        coalesce(dbt_valid_to, cast('{{ var("high_date") }}' as timestamp)) as valid_to_po
    from {{ ref('stg_product_order') }}
    
    {% if var("current_records_only") %}

    where valid_to = cast('{{ var("high_date") }}' as timestamp)

    {% endif %}
)

, build_spine as (

    select
        product_important_status.*,
        product_order.product_order_id
    from product_important_status
    left join
       (select product_id, product_order_id from product_order group by 1,2) as product_order 
        on product_important_status.product_id = product_order.product_id
)

, snap_join as  (
    select
        product_order.order_id,
        product_order.order_status,
        product_order.valid_to_po,
        product_order.valid_from_po,
    {{ join_snapshots(
            cte_join='build_spine', 
            cte_join_on='product_order', 
            cte_join_valid_to='valid_to_pis',
            cte_join_valid_from='valid_from_pis', 
            cte_join_on_valid_to='valid_to_po', 
            cte_join_on_valid_from='valid_from_po',
            cte_join_id='product_order_id', 
            cte_join_on_id='product_order_id'
    ) }}
)

, rename as (
    select 
        product_id,
        product_order_id,
        order_id,
        important_status,
        order_status,

        -- these 2 columns are the valid ranges from the resulting join_snapshots -- todo make this dynamic
        add_product_order_valid_from as valid_from,
        add_product_order_valid_to as valid_to,

        -- only showing these columns to show the join windows, in prod wouldnt show the columns below
        valid_from_pis,
        valid_to_pis
        valid_from_po,
        valid_to_po
    from snap_join
)
select *
from rename