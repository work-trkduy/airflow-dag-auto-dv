{%- from "macros/extract_name_tables.sql" import
    render_target_table_full_name,
    render_target_snp_table_full_name -%}
{%- from "macros/extract_name_columns.sql" import
    render_hash_key_lsat_name,
    render_hash_key_lnk_name,
    render_hash_diff_name,
    render_list_dependent_key_name,
    render_list_attr_column_name,
    render_list_dv_system_column_name -%}

{#---------------------------------------#}

{%- set hkey_lnk_name = render_hash_key_lnk_name(model) -%}
{%- set dep_keys = render_list_dependent_key_name(model) | from_json -%}

with cte_stg_lsat_snp as (
{% include 'models/lsat/lsat-snp_transform.sql' %}
)
merge into {{render_target_snp_table_full_name(target_schema, model, target_type)}} tgt
using cte_stg_lsat_snp src
on tgt.{{hkey_lnk_name}} = src.{{hkey_lnk_name}}
    {%- for key in dep_keys %}
    and tgt.{{key}} = src.{{key}}
    {%- endfor %}
when matched then update set *
when not matched then insert *