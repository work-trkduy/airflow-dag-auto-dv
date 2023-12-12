{%- from "macros/extract_name_tables.sql" import
    render_target_der_table_full_name,
    render_source_table_view_name -%}
{%- from "macros/extract_name_source_columns.sql" import
    render_list_hash_key_hub_component,
    render_list_source_dependent_key_name,
    render_list_source_ldt_key_name -%}
{%- from "macros/derive_columns.sql" import
    render_hash_key_sat_treatment,
    render_hash_key_hub_treatment,
    render_hash_diff_treatment,
    render_list_dependent_key_treatment,
    render_list_attr_column_treatment,
    render_list_dv_system_column_treatment -%}

{#---------------------------------------#}

{%- set src_hkey_hub = render_list_hash_key_hub_component(model) | from_json -%}
{%- set src_dep_keys = render_list_source_dependent_key_name(model) | from_json -%}
{%- set src_ldt_keys = render_list_source_ldt_key_name(dv_system) | from_json -%}

select
    {{render_hash_key_sat_treatment(model, dv_system)}},
    {{render_hash_key_hub_treatment(model)}},
    {{render_hash_diff_treatment(model)}},
    {% for column in render_list_dependent_key_treatment(model) | from_json -%}
    {{column}},
    {% endfor -%}
    {{render_list_attr_column_treatment(model) | from_json | join(',\n\t')}},
    {{render_list_dv_system_column_treatment(dv_system) | from_json | join(',\n\t')}}
from {{render_source_table_view_name(model)}}
where
    {{src_ldt_keys[0]}} >= $v_end_date
    {% for column in src_hkey_hub + src_dep_keys -%}
    and {{column}} is not null
    {% endfor -%}