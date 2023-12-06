{%- from "macros/extract_name_tables.sql" import
    render_target_table_full_name,
    render_target_lsate_table_full_name,
    render_source_table_view_name -%}
{%- from "macros/extract_name_columns.sql" import
    render_hash_key_lnk_name,
    render_hash_key_drv_name,
    render_list_dv_system_column_name,
    render_list_dv_system_ldt_key_name -%}
{%- from "macros/extract_name_source_columns.sql" import
    render_list_hash_key_lnk_component,
    render_list_source_ldt_key_name -%}
{%- from "macros/derive_columns.sql" import
    render_hash_key_lnk_treatment,
    render_hash_key_drv_treatment,
    render_list_dv_system_column_treatment -%}

{#---------------------------------------#}

{%- if (model.get('columns') | selectattr("key_type", "equalto", "hash_key_drv") | list | length) > 0 -%}

{%- set lnk_table_name = render_target_table_full_name(target_schema, model) -%}
{%- set lsate_table_name = render_target_lsate_table_full_name(target_schema, model) -%}

{%- set hkey_lnk_name = render_hash_key_lnk_name(model) -%}
{%- set hkey_drv_name = render_hash_key_drv_name(model) -%}
{%- set ldt_keys = render_list_dv_system_ldt_key_name(dv_system) | from_json -%}
{%- set src_ldt_keys = render_list_source_ldt_key_name(dv_system) | from_json -%}

{#---------------------------------------#}

with cte_stg_lsate as (
    select
        {{render_hash_key_lnk_treatment(model, collision_code)}},
        {{render_list_hash_key_lnk_component(model) | from_json | join(' is not null and ')}} is not null as hkey_lnk_not_null,
        {{render_hash_key_drv_treatment(model, collision_code)}},
        {{render_list_dv_system_column_treatment(dv_system) | from_json | join(',\n\t\t')}},
        {{src_ldt_keys[0]}} as dv_startts,
        1 as from_stg
    from {{render_source_table_view_name(model)}}
),
cte_current_effectivity as (
    select
        {{hkey_lnk_name}},
        {{hkey_drv_name}},
        {{render_list_dv_system_column_name(dv_system) | from_json | join(', ')}},
        dv_startts,
        0 as from_stg
    from (
        select
            lsate.*,
            lnk.{{hkey_drv_name}},
            row_number() over (
                partition by lsate.{{hkey_lnk_name}}
                order by lsate.{{ ldt_keys | join(' desc, lsate.') }} desc
            ) as row_num
        from {{lsate_table_name}} lsate
        join {{lnk_table_name}} lnk
            on lsate.{{hkey_lnk_name}} = lnk.{{hkey_lnk_name}}
    )
    where row_num = 1 and dv_endts = timestamp'9999-12-31'
),
cte_stg_hkey_drv as (
    select
        {{hkey_lnk_name}},
        {{hkey_drv_name}},
        {{render_list_dv_system_column_name(dv_system) | from_json | join(', ')}},
        dv_startts
    from (
        select
            *,
            row_number() over (
                partition by lsate.{{hkey_drv_name}}
                order by {{ ldt_keys | join(' asc, ') }} asc
            ) as row_num
        from cte_stg_lsate lsate
    )
    where row_num = 1
),
cte_new_effectivity as (
    select
        {{hkey_lnk_name}},
        dv_startts,
        lead(dv_startts, 1, timestamp'9999-12-31') over (
            partition by {{hkey_drv_name}}
            order by {{ ldt_keys | join(' asc, ') }} asc
        ) dv_endts,
        {{render_list_dv_system_column_name(dv_system) | from_json | join(', ')}}
    from (
        select
            *,
            lag({{hkey_lnk_name}}, 1, null) over (
                partition by {{hkey_drv_name}}
                order by {{ ldt_keys | join(' asc, ') }} asc
            ) as prev_hkey_lnk
        from (
            select * from cte_current_effectivity
            union all
            select
                {{hkey_lnk_name}},
                {{hkey_drv_name}},
                {{render_list_dv_system_column_name(dv_system) | from_json | join(', ')}},
                dv_startts,
                from_stg
            from cte_stg_lsate
            where hkey_lnk_not_null
        )
    )
    where not (prev_hkey_lnk <=> {{hkey_lnk_name}}) and from_stg = 1
),
cte_close_effectivity as (
    select
        curr.{{hkey_lnk_name}},
        curr.dv_startts,
        stg.dv_startts as dv_endts,
        {% for column in render_list_dv_system_column_name(dv_system) | from_json -%}
        stg.{{column}} {{-',' if not loop.last}}
        {% endfor -%}
    from cte_current_effectivity curr
    join cte_stg_hkey_drv stg
        on curr.{{hkey_drv_name}} = stg.{{hkey_drv_name}}
        and curr.{{hkey_lnk_name}} != stg.{{hkey_lnk_name}}
)
select * from cte_new_effectivity
union all
select * from cte_close_effectivity

{%- endif -%}