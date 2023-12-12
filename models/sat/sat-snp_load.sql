{%- from "macros/extract_name_tables.sql" import
    render_target_table_full_name,
    render_target_snp_table_full_name -%}
{%- from "macros/extract_name_columns.sql" import
    render_hash_key_sat_name,
    render_hash_key_hub_name,
    render_hash_diff_name,
    render_list_dependent_key_name,
    render_list_attr_column_name,
    render_list_dv_system_column_name -%}

{#---------------------------------------#}

{%- set hkey_hub_name = render_hash_key_hub_name(model) -%}
{%- set dep_keys = render_list_dependent_key_name(model) | from_json -%}

with cte_stg_sat_snp as (
{% include 'models/sat/sat-snp_transform.sql' %}
)
merge into {{render_target_snp_table_full_name(model)}} tgt
using cte_stg_sat_snp src
on tgt.{{hkey_hub_name}} = src.{{hkey_hub_name}}
    {%- for key in dep_keys %}
    and tgt.{{key}} = src.{{key}}
    {%- endfor %}
when matched then update set *
when not matched then insert *