select 
    {{ build_key_from_columns(table_name=ref('product_important_status_snapshot'), columns=['entity_id','important_status']) }} as grain_id,
    *
from {{ ref('product_important_status_snapshot') }}