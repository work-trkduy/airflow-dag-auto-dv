{%- macro _render_hash_component_transformation(source_column, error_code = "'-1'", upper = False) -%}
    {%- if upper -%}
        coalesce(nullif(rtrim(upper(cast({{source_column}} as string))), ''), {{error_code}})
    {%- else -%}
        coalesce(nullif(rtrim(cast({{source_column}} as string)), ''), {{error_code}})
    {%- endif -%}
{%- endmacro -%}

{%- macro _render_hash_key_transformation(columns, collision_code) -%}
    sha2(
        {%- for column in columns -%}
            {%- if column.get('key_type') in ("hash_key_hub", "hash_key_drv", "hash_key_lnk") -%}
                {%- for source_column in column.get('source') -%}
                    {{_render_hash_component_transformation(source_column, upper = True)}} {%- if not loop.last %} || '#~!' || {% endif -%}
                {%- endfor -%}
                || '#~!' || '{{collision_code}}'
            {%- else -%}
                {%- for source_column in column.get('source') -%}
                    {{_render_hash_component_transformation(source_column, upper = False)}} {%- if not loop.last %} || '#~!' || {% endif -%}
                {%- endfor -%}
            {%- endif -%}
            {%- if not loop.last %} || '#~!' || {% endif -%}
        {%- endfor -%}
    , 256)
{%- endmacro -%}

{%- macro render_list_biz_key_treatment(model) %}
    {%- set outs = [] -%}
    {%- for column in model.get('columns') | selectattr("key_type", "equalto", "biz_key") -%}
        {%- set tmp -%}
            {{_render_hash_component_transformation(column.get('source')|first, upper=True)}} as {{column.get('target')}}
        {%- endset -%}
        {% do outs.append(tmp) %}
    {%- endfor -%}
    {{ outs | to_json }}
{%- endmacro -%}

{%- macro render_hash_key_hub_treatment(model, collision_code) %}
    {%- set column = model.get('columns') | selectattr("key_type", "equalto", "hash_key_hub") | first -%}
    {{_render_hash_key_transformation([column], collision_code)}} as {{column.get('target')}}
{%- endmacro -%}

{%- macro render_hash_key_drv_treatment(model, collision_code) %}
    {%- set column = model.get('columns') | selectattr("key_type", "equalto", "hash_key_drv") | first -%}
    {{_render_hash_key_transformation([column], collision_code)}} as {{column.get('target')}}
{%- endmacro -%}

{%- macro render_list_hash_key_hub_treatment(model, collision_code) %}
    {%- set outs = [] -%}
    {%- for column in model.get('columns') -%}
        {%- if column.get('key_type') in ("hash_key_hub", "hash_key_drv") -%}
            {%- set tmp -%}
                {{_render_hash_key_transformation([column], collision_code)}} as {{column.get('target')}}
            {%- endset -%}
            {% do outs.append(tmp) %}
        {%- endif -%}
    {%- endfor -%}
    {{ outs | to_json }}
{%- endmacro -%}

{%- macro render_hash_key_lnk_treatment(model, collision_code) %}
    {%- set column = model.get('columns') | selectattr("key_type", "equalto", "hash_key_lnk") | first -%}
    {{_render_hash_key_transformation([column], collision_code)}} as {{column.get('target')}}
{%- endmacro -%}

{%- macro render_hash_key_sat_treatment(model, collision_code, dv_system) %}
    {%- set columns = model.get('columns') | selectattr("key_type", "equalto", "hash_key_hub") | list -%}
    {%- do columns.extend(model.get('columns') | selectattr("key_type", "equalto", "dependent_key") | list) -%}
    {%- for key in ('dv_src_ldt', 'dv_kaf_ldt', 'dv_kaf_ofs') -%}
        {%- set tmp = (dv_system.get('columns') | selectattr('target', 'equalto', key) | first).copy() -%}
        {%- do tmp.update({'source': [tmp.get('source')]}) -%}
        {%- do columns.append(tmp) -%}
    {%- endfor -%}

    {%- set target = (model.get('columns') | selectattr("key_type", "equalto", "hash_key_sat") | first).get('target') -%}
    {{_render_hash_key_transformation(columns, collision_code)}} as {{target}}
{%- endmacro -%}

{%- macro render_hash_key_lsat_treatment(model, collision_code, dv_system) %}
    {%- set columns = model.get('columns') | selectattr("key_type", "equalto", "hash_key_lnk") | list -%}
    {%- do columns.extend(model.get('columns') | selectattr("key_type", "equalto", "dependent_key") | list) -%}
    {%- for key in ('dv_src_ldt', 'dv_kaf_ldt', 'dv_kaf_ofs') -%}
        {%- set tmp = (dv_system.get('columns') | selectattr('target', 'equalto', key) | first).copy() -%}
        {%- do tmp.update({'source': [tmp.get('source')]}) -%}
        {%- do columns.append(tmp) -%}
    {%- endfor -%}

    {%- set target = (model.get('columns') | selectattr("key_type", "equalto", "hash_key_sat") | first).get('target') -%}
    {{_render_hash_key_transformation(columns, collision_code)}} as {{target}}
{%- endmacro -%}

{%- macro render_hash_diff_treatment(model, collision_code) %}
    {%- set column = model.get('columns') | selectattr("key_type", "equalto", "hash_diff") | first -%}
    {%- if 'source' not in column -%}
        {%- set column = column.copy() -%}
        {%- do column.update({'source': []}) -%}
        {%- for attr_column in model.get('columns') | selectattr('key_type', 'undefined') -%}
            {% do column.get('source').append(attr_column.get('source')|first) %}
        {%- endfor -%}
    {%- endif -%}

    sha2(
        {%- for source_column in column.get('source') -%}
            {{_render_hash_component_transformation(source_column, error_code="repeat('0',16)")}} {%- if not loop.last %} || '#~!' || {% endif -%}
        {%- endfor -%}
    , 256) as {{column.get('target')}}
{%- endmacro -%}

{%- macro render_list_dependent_key_treatment(model) %}
    {%- set outs = [] -%}
    {%- for column in model.get('columns') | selectattr('key_type', 'equalto', "dependent_key") -%}
        {%- set tmp -%}
            {%- if column.get('source')|first == column.get('target') -%}
                {{column.get('target')}}
            {%- else -%}
                {{column.get('source')|first}} as {{column.get('target')}}
            {%- endif -%}
        {%- endset -%}
        {% do outs.append(tmp) %}
    {%- endfor -%}
    {{ outs | to_json }}
{%- endmacro -%}

{%- macro render_list_attr_column_treatment(model) %}
    {%- set outs = [] -%}
    {%- for column in model.get('columns') | selectattr('key_type', 'undefined') -%}
        {%- set tmp -%}
            {%- if column.get('source')|first == column.get('target') -%}
                {{column.get('target')}}
            {%- else -%}
                {{column.get('source')|first}} as {{column.get('target')}}
            {%- endif -%}
        {%- endset -%}
        {% do outs.append(tmp) %}
    {%- endfor -%}
    {{ outs | to_json }}
{%- endmacro -%}

{%- macro render_list_dv_system_column_treatment(dv_system) %}
    {%- set outs = [] -%}
    {%- for column in dv_system.get('columns') -%}
        {%- set tmp -%}
            {{column.get('source')}} as {{column.get('target')}}
        {%- endset -%}
        {% do outs.append(tmp) %}
    {%- endfor -%}
    {{ outs | to_json }}
{%- endmacro -%}