{%- from "macros/extract_name_tables.sql" import
    render_target_table_full_name,
    render_source_table_view_name -%}
{%- from "macros/extract_name_columns.sql" import
    render_hash_key_sat_name,
    render_hash_key_hub_name,
    render_hash_diff_name,
    render_list_dependent_key_name,
    render_list_attr_column_name,
    render_list_dv_system_column_name,
    render_list_dv_system_ldt_key_name -%}
{%- from "macros/derive_columns.sql" import
    render_hash_key_sat_treatment,
    render_hash_key_hub_treatment,
    render_hash_diff_treatment,
    render_list_dependent_key_treatment,
    render_list_attr_column_treatment,
    render_list_dv_system_column_treatment -%}

{#---------------------------------------#}

{%- set hkey_hub_name = render_hash_key_hub_name(model) -%}
{%- set dep_keys = render_list_dependent_key_name(model) | from_json -%}
{%- set ldt_keys = render_list_dv_system_ldt_key_name(dv_system) | from_json -%}

with cte_stg_sat as (
    select
        {{render_hash_key_sat_treatment(model, collision_code, dv_system)}},
        {{render_hash_key_hub_treatment(model, collision_code)}},
        {{render_hash_diff_treatment(model)}},
        {% for column in render_list_dependent_key_treatment(model) | from_json -%}
        {{column}},
        {% endfor -%}
        {{render_list_attr_column_treatment(model) | from_json | join(',\n\t\t')}},
        {{render_list_dv_system_column_treatment(dv_system) | from_json | join(',\n\t\t')}},
        '{{collision_code}}' as dv_ccd,
        -1 as row_num
    from {{render_source_table_view_name(model)}}
),
cte_sat_set_row_num as (
    select
        {{render_hash_key_sat_name(model)}},
        {{render_hash_key_hub_name(model)}},
        {{render_hash_diff_name(model)}},
        {% for column in render_list_dependent_key_name(model) | from_json -%}
        {{column}},
        {% endfor -%}
        {{render_list_attr_column_name(model) | from_json | join(',\n\t\t')}},
        {{render_list_dv_system_column_name(dv_system) | from_json | join(',\n\t\t')}},
        dv_ccd,
        row_number() over (
            partition by {{hkey_hub_name}} {{-', ' + dep_keys|join(', ') if dep_keys|length > 0}}
            order by {% for key in ldt_keys -%}
                {{key}} desc {{-', ' if not loop.last}}
                {%- endfor %}
        ) as row_num
    from {{render_target_table_full_name(target_schema, model)}} tgt
    where exists (
        select 1
        from cte_stg_sat src
        where tgt.{{hkey_hub_name}} = src.{{hkey_hub_name}}
            {% for key in dep_keys -%}
                and tgt.{{key}} = src.{{key}}
            {% endfor -%}
    )
),
cte_sat_latest_records as (
    select *
    from cte_sat_set_row_num
    where row_num = 1
),
cte_stg_sat_dedup_hsh_dif as (
    select *
    from (
        select
            *,
            lag(dv_hsh_dif, 1, null) over w as prev_hash_diff,
            lag(dv_cdc_ops, 1, null) over w as prev_cdc_ops
        from (
            select * from cte_stg_sat
            union
            select * from cte_sat_latest_records
        )
        window w as (
            partition by {{hkey_hub_name}} {{-', ' + dep_keys|join(', ') if dep_keys|length > 0}}
            order by {% for key in ldt_keys -%}
                {{key}} asc {{-', ' if not loop.last}}
                {%- endfor %}
        )
    )
    where
        prev_hash_diff != dv_hsh_dif or
        prev_hash_diff is null or
        lower(dv_cdc_ops) = lower('d') or
        lower(prev_cdc_ops) = lower('d')
)
select
    {{render_hash_key_sat_name(model)}},
    {{render_hash_key_hub_name(model)}},
    {{render_hash_diff_name(model)}},
    {% for column in render_list_dependent_key_name(model) | from_json -%}
    {{column}},
    {% endfor -%}
    {{render_list_attr_column_name(model) | from_json | join(',\n\t')}},
    {{render_list_dv_system_column_name(dv_system) | from_json | join(',\n\t')}},
    dv_ccd
from cte_stg_sat_dedup_hsh_dif
where row_num = -1