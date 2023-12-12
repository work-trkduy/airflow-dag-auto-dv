{%- from "macros/extract_name_tables.sql" import
    render_target_lsate_table_full_name,
    render_tbl_partition,
    render_tblproperties -%}
{%- from "macros/extract_name_columns.sql" import
    render_hash_key_lnk_name,
    render_list_dv_system_column_name -%}

{#---------------------------------------#}

{%- if (model.get('columns') | selectattr("key_type", "equalto", "hash_key_drv") | list | length) > 0 -%}

insert into {{render_target_lsate_table_full_name(model)}} (
    {{render_hash_key_lnk_name(model)}},
    dv_startts,
    dv_endts,
    {{render_list_dv_system_column_name(dv_system) | from_json | join(',\n\t')}}
)
{% include 'models/lnk/lsate_transform.sql' %}

{%- endif -%}