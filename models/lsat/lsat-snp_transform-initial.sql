{%- from "macros/extract_name_tables.sql" import
    render_source_table_view_name -%}
{%- from "macros/extract_name_columns.sql" import
    render_hash_key_lsat_name,
    render_hash_key_lnk_name,
    render_hash_diff_name,
    render_list_dependent_key_name,
    render_list_attr_column_name,
    render_list_dv_system_column_name,
    render_list_dv_system_ldt_key_name -%}
{%- from "macros/extract_name_source_columns.sql" import
    render_list_hash_key_lnk_component,
    render_list_source_dependent_key_name,
    render_list_source_ldt_key_name -%}
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
{%- set src_hkey_lnk = render_list_hash_key_lnk_component(model) | from_json -%}
{%- set src_dep_keys = render_list_source_dependent_key_name(model) | from_json -%}
{%- set src_ldt_keys = render_list_source_ldt_key_name(dv_system) | from_json -%}

with cte_stg_lsat as (
    select
        {{render_hash_key_lsat_treatment(model, collision_code, dv_system)}},
        {{render_hash_key_lnk_treatment(model, collision_code)}},
        {{render_hash_diff_treatment(model)}},
        {% for column in render_list_dependent_key_treatment(model) | from_json -%}
        {{column}},
        {% endfor -%}
        {{render_list_attr_column_treatment(model) | from_json | join(',\n\t\t')}},
        {{render_list_dv_system_column_treatment(dv_system) | from_json | join(',\n\t\t')}}
    from {{render_source_table_view_name(model)}}
    where
        {{src_ldt_keys[0]}} < $v_end_date
        {% for column in src_hkey_lnk + src_dep_keys -%}
        and {{column}} is not null
        {% endfor -%}
),
cte_stg_lsat_set_row_num as (
    select *,
        row_number() over (
            partition by {{hkey_lnk_name}} {{-', ' + dep_keys|join(', ') if dep_keys|length > 0}}
            order by {% for key in ldt_keys -%}
                {{key}} desc {{-', ' if not loop.last}}
                {%- endfor %}
        ) as row_num
    from cte_stg_lsat
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
from cte_stg_lsat_set_row_num
where row_num = 1