{%- from "macros/extract_name_tables.sql" import
    render_target_table_full_name,
    render_tblproperties,
    render_tbl_partition -%}
{%- from "macros/extract_name_columns.sql" import
    render_hash_key_hub_name,
    render_list_biz_key_name,
    render_list_dv_system_column_name -%}

{#---------------------------------------#}

create table if not exists {{render_target_table_full_name(target_schema, model)}} (
    {{render_hash_key_hub_name(model, with_data_type = true)}},
    {{render_list_biz_key_name(model, with_data_type = true) | from_json | join(',\n\t')}},
    {{render_list_dv_system_column_name(dv_system, with_data_type = true) | from_json | join(',\n\t')}},
    dv_ccd string
)
using iceberg
{{render_tblproperties(tbl_properties)}}
{{render_tbl_partition(model)}}