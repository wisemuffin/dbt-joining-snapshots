{% macro build_key_from_columns(table_name, columns) %}

{% set col_list = [] %}



{%- for col in columns -%}

    {%- do col_list.append("coalesce(cast(" ~ col ~ " as " ~ dbt_utils.type_string() ~ "), '')")  -%}

{%- endfor -%}

{{ log("Running build_key_from_columns with col_list: " ~ col_list) }}

{{ return(dbt_utils.surrogate_key(col_list)) }}

{% endmacro %}