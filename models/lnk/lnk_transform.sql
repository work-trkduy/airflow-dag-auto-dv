{%- from "macros/extract_name_tables.sql" import
    render_target_table_full_name,
    render_source_table_view_name -%}
{%- from "macros/extract_name_columns.sql" import
    render_hash_key_lnk_name,
    render_list_hash_key_hub_name,
    render_list_dv_system_column_name,
    render_list_dv_system_ldt_key_name -%}
{%- from "macros/extract_name_source_columns.sql" import
    render_list_hash_key_lnk_component -%}
{%- from "macros/derive_columns.sql" import
    render_hash_key_lnk_treatment,
    render_list_hash_key_hub_treatment,
    render_list_dv_system_column_treatment -%}

{#---------------------------------------#}

{%- set hkey_name = render_hash_key_lnk_name(model) -%}
{%- set ldt_keys = render_list_dv_system_ldt_key_name(dv_system) | from_json -%}

with cte_stg_lnk as (
    select
        {{render_hash_key_lnk_treatment(model, collision_code)}},
        {{render_list_hash_key_hub_treatment(model) | from_json | join(',\n\t')}},
        {{render_list_dv_system_column_treatment(dv_system) | from_json | join(',\n\t')}}
    from {{render_source_table_view_name(model)}}
    where {{render_list_hash_key_lnk_component(model) | from_json | join(' is not null and ')}} is not null
),
cte_stg_lnk_latest_records as (
select * from (
    select
        *,
        row_number() over (
            partition by {{hkey_name}}
            order by {% for key in ldt_keys -%}
                {{key}} asc {%- if not loop.last -%}, {% endif -%}
                {%- endfor %}
        ) as row_num
    from cte_stg_lnk
)
where row_num = 1
),
cte_stg_lnk_existed_keys (
    select {{hkey_name}}
    from cte_stg_lnk src
    where exists (
        select 1
        from {{render_target_table_full_name(target_schema, model)}} tgt
        where tgt.{{hkey_name}} = src.{{hkey_name}}
    )
)
select
    {{hkey_name}},
    {{render_list_hash_key_hub_name(model, with_data_type = false) | from_json | join(',\n\t')}},
    {{render_list_dv_system_column_name(dv_system, with_data_type = false) | from_json | join(',\n\t')}}
from cte_stg_lnk_latest_records src
where not exists (
    select 1
    from cte_stg_lnk_existed_keys tgt
    where tgt.{{hkey_name}} = src.{{hkey_name}}
)