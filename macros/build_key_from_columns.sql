{% macro build_key_from_columns(table_name, exclude=[]) %}

{% set cols = dbt_utils.star(from=table_name, except = exclude) %}

{% set input_col_list = cols.split(',') %}

{% set output_col_list = [] %}

{%- for col in input_col_list -%}

    {%- do output_col_list.append("coalesce(cast(" ~ col ~ " as " ~ dbt_utils.type_string() ~ "), '')")  -%}

{%- endfor -%}

{{ log("Running build_key_from_columns with output_col_list: " ~ output_col_list) }}

{{ return(dbt_utils.surrogate_key(output_col_list)) }}

{% endmacro %}