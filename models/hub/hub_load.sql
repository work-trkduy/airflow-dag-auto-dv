{%- from "macros/extract_name_tables.sql" import
    render_target_table_full_name -%}
{%- from "macros/extract_name_columns.sql" import
    render_collision_code_name,
    render_hash_key_hub_name,
    render_list_biz_key_name,
    render_list_dv_system_column_name -%}

{#---------------------------------------#}

insert into {{render_target_table_full_name(model)}} (
    {{render_hash_key_hub_name(model)}},
    {{render_list_biz_key_name(model) | from_json | join(',\n\t')}},
    {{render_list_dv_system_column_name(dv_system) | from_json | join(',\n\t')}},
    {{render_collision_code_name()}}
)
{% include 'models/hub/hub_transform.sql' %}