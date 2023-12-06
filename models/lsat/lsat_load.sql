{%- from "macros/extract_name_tables.sql" import
    render_target_table_full_name -%}
{%- from "macros/extract_name_columns.sql" import
    render_hash_key_lsat_name,
    render_hash_key_lnk_name,
    render_hash_diff_name,
    render_list_dependent_key_name,
    render_list_attr_column_name,
    render_list_dv_system_column_name -%}

{#---------------------------------------#}

insert into {{render_target_table_full_name(target_schema, model)}} (
    {{render_hash_key_lsat_name(model)}},
    {{render_hash_key_lnk_name(model)}},
    {{render_hash_diff_name(model)}},
    {% for column in render_list_dependent_key_name(model) | from_json -%}
    {{column}},
    {% endfor -%}
    {{render_list_attr_column_name(model) | from_json | join(',\n\t')}},
    {{render_list_dv_system_column_name(dv_system) | from_json | join(',\n\t')}}
)
{% include 'models/lsat/lsat_transform.sql' %}