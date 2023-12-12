{%- from "macros/extract_name_tables.sql" import
    render_target_table_full_name,
    render_target_der_table_full_name -%}
{%- from "macros/extract_name_columns.sql" import
    render_hash_key_sat_name,
    render_hash_key_hub_name,
    render_hash_diff_name,
    render_list_dependent_key_name,
    render_list_attr_column_name,
    render_list_dv_system_column_name,
    render_list_dv_system_ldt_key_name,
    render_dv_system_cdc_ops_name -%}
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
{%- set cdc_ops = render_dv_system_cdc_ops_name(dv_system) -%}

with cte_sat_der_latest_records as (
    select * from (
        select
            *,
            row_number() over (
                partition by {{hkey_hub_name}} {{-', ' + dep_keys|join(', ') if dep_keys|length > 0}}
                order by {% for key in ldt_keys -%} {{key}} desc {{-', ' if not loop.last-}} {%- endfor -%}
            ) as row_num
        from {{render_target_der_table_full_name(model)}}
        where {{ldt_keys[0]}} >= $v_from_date and {{ldt_keys[0]}} < $v_end_date
    )
    where row_num = 1
),
cte_sat_latest_records as (
    select * from (
        select
            *,
            row_number() over (
                partition by {{hkey_hub_name}} {{-', ' + dep_keys|join(', ') if dep_keys|length > 0}}
                order by {% for key in ldt_keys -%} {{key}} desc {{-', ' if not loop.last-}} {%- endfor -%}
            ) as row_num
        from {{render_target_table_full_name(model)}}
    )
    where row_num = 1
)
select
    {{render_hash_key_sat_name(model)}},
    {{render_hash_key_hub_name(model)}},
    {{render_hash_diff_name(model)}},
    {% for column in render_list_dependent_key_name(model) | from_json -%}
    {{column}},
    {% endfor -%}
    {{render_list_attr_column_name(model) | from_json | join(',\n\t')}},
    {{render_list_dv_system_column_name(dv_system) | from_json | join(',\n\t')}}
from cte_sat_der_latest_records sat_der
where not exists (
    select 1 from cte_sat_latest_records sat
    where
        sat_der.{{hkey_hub_name}} = sat.{{hkey_hub_name}}
        {% for column in dep_keys -%}
        and sat_der.{{column}} = sat.{{column}}
        {% endfor -%}
        and lower(sat_der.{{cdc_ops}}) != 'd'
        and lower(sat.{{cdc_ops}}) != 'd'
)