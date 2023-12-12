{%- from "macros/extract_name_tables.sql" import
    render_target_table_full_name -%}
{%- from "macros/extract_name_columns.sql" import
    render_hash_key_lnk_name,
    render_list_hash_key_hub_name,
    render_list_dv_system_column_name,
    render_list_dv_system_ldt_key_name -%}

{#---------------------------------------#}

insert into {{render_target_table_full_name(model)}} (
    {{render_hash_key_lnk_name(model, with_dtype = false)}},
    {{render_list_hash_key_hub_name(model, with_dtype = false) | from_json | join(',\n\t')}},
    {{render_list_dv_system_column_name(dv_system, with_dtype = false) | from_json | join(',\n\t')}}
)
{% include 'models/lnk/lnk_transform.sql' %}