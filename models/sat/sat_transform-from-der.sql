{%- from "macros/extract_name_tables.sql" import
    render_target_table_full_name,
    render_target_der_table_full_name,
    render_target_snp_table_full_name,
    render_batch_job_log_table_full_name -%}
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

with cte_latest_datelastmaint as (
    select datelastmaint
    from (
        select *, row_number() over (partition by 1 order by run_date desc) as row_num
        from {{render_batch_job_log_table_full_name()}}
    ) a
    where row_num = 1
),
cte_sat_der_set_row_num as (
    select
        sat_der.*,
        row_number() over (
            partition by {{hkey_hub_name}} {{-', ' + dep_keys|join(', ') if dep_keys|length > 0}}
            order by {% for key in ldt_keys -%} {{key}} desc {{-', ' if not loop.last-}} {%- endfor -%}
        ) as row_num
    from {{render_target_der_table_full_name(target_schema, model, target_type)}} sat_der
    join cte_latest_datelastmaint log on sat_der.dv_src_ldt <= log.datelastmaint
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
from cte_sat_der_set_row_num sat_der
where row_num != 1 or not exists (
    select 1 from {{render_target_snp_table_full_name(target_schema, model, target_type)}} sat_snp
    where sat_der.{{hkey_hub_name}} = sat_snp.{{hkey_hub_name}}
        {% for column in dep_keys -%}
        and sat_der.{{column}} = sat_snp.{{column}}
        {% endfor -%}
)