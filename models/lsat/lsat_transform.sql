{%- from "macros/extract_name_tables.sql" import
    render_target_table_full_name,
    render_target_der_table_full_name -%}
{%- from "macros/extract_name_columns.sql" import
    render_hash_key_lsat_name,
    render_hash_key_lnk_name,
    render_hash_diff_name,
    render_list_dependent_key_name,
    render_list_attr_column_name,
    render_list_dv_system_column_name,
    render_list_dv_system_ldt_key_name,
    render_dv_system_cdc_ops_name -%}
{%- from "macros/derive_columns.sql" import
    render_hash_key_lsat_treatment,
    render_hash_key_lnk_treatment,
    render_hash_diff_treatment,
    render_list_dependent_key_treatment,
    render_list_attr_column_treatment,
    render_list_dv_system_column_treatment -%}

{#---------------------------------------#}

{%- set hkey_lnk_name = render_hash_key_lnk_name(model) -%}
{%- set dep_keys = render_list_dependent_key_name(model) | from_json -%}
{%- set ldt_keys = render_list_dv_system_ldt_key_name(dv_system) | from_json -%}
{%- set cdc_ops = render_dv_system_cdc_ops_name(dv_system) -%}

with cte_lsat_der_latest_records as (
    select * from (
        select
            *,
            row_number() over (
                partition by {{hkey_lnk_name}} {{-', ' + dep_keys|join(', ') if dep_keys|length > 0}}
                order by {% for key in ldt_keys -%} {{key}} desc {{-', ' if not loop.last-}} {%- endfor -%}
            ) as row_num
        from {{render_target_der_table_full_name(target_schema, model, target_type)}}
        where {{ldt_keys[0]}} >= $v_from_date and {{ldt_keys[0]}} < $v_end_date
    )
    where row_num = 1
),
cte_lsat_latest_records as (
    select * from (
        select
            *,
            row_number() over (
                partition by {{hkey_lnk_name}} {{-', ' + dep_keys|join(', ') if dep_keys|length > 0}}
                order by {% for key in ldt_keys -%} {{key}} desc {{-', ' if not loop.last-}} {%- endfor -%}
            ) as row_num
        from {{render_target_table_full_name(target_schema, model)}}
    )
    where row_num = 1
)
select
    {{render_hash_key_lsat_name(model)}},
    {{render_hash_key_lnk_name(model)}},
    {{render_hash_diff_name(model)}},
    {% for column in render_list_dependent_key_name(model) | from_json -%}
    {{column}},
    {% endfor -%}
    {{render_list_attr_column_name(model) | from_json | join(',\n\t')}},
    {{render_list_dv_system_column_name(dv_system) | from_json | join(',\n\t')}}
from cte_lsat_der_latest_records lsat_der
where not exists (
    select 1 from cte_lsat_latest_records lsat
    where
        lsat_der.{{hkey_lnk_name}} = lsat.{{hkey_lnk_name}}
        {% for column in dep_keys -%}
        and lsat_der.{{column}} = lsat.{{column}}
        {% endfor -%}
        and lower(lsat_der.{{cdc_ops}}) != 'd'
        and lower(lsat.{{cdc_ops}}) != 'd'
)