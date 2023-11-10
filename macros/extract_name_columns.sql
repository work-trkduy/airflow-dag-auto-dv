{%- macro render_hash_key_hub_name(models, with_data_type=false) -%}
    {%- set column = models.get('columns') | selectattr("key_type", "equalto", "hash_key_hub") | first -%}
    {{column.get('target')}} {%- if with_data_type %} {{column.get('data_type')}} {%- endif -%}
{%- endmacro -%}

{%- macro render_list_hash_key_hub_name(models, with_data_type=false) -%}
    {%- set outs = [] -%}
    {%- for column in models.get('columns') | selectattr("key_type", "equalto", "hash_key_hub") | list + models.get('columns') | selectattr("key_type", "equalto", "hash_key_drv") | list -%}
        {%- set tmp -%}
            {{column.get('target')}} {%- if with_data_type %} {{column.get('data_type')}} {%- endif -%}
        {%- endset -%}
        {% do outs.append(tmp) %}
    {%- endfor -%}
    {{ outs | to_json }}
{%- endmacro -%}

{%- macro render_hash_key_lnk_name(models, with_data_type=false) -%}
    {%- set column = models.get('columns') | selectattr("key_type", "equalto", "hash_key_lnk") | first -%}
    {{column.get('target')}} {%- if with_data_type %} {{column.get('data_type')}} {%- endif -%}
{%- endmacro -%}

{%- macro render_hash_key_drv_name(models, with_data_type=false) -%}
    {%- set column = models.get('columns') | selectattr("key_type", "equalto", "hash_key_drv") | first -%}
    {{column.get('target')}} {%- if with_data_type %} {{column.get('data_type')}} {%- endif -%}
{%- endmacro -%}

{%- macro render_hash_key_sat_name(models, with_data_type=false) -%}
    {%- set column = models.get('columns') | selectattr("key_type", "equalto", "hash_key_sat") | first -%}
    {{column.get('target')}} {%- if with_data_type %} {{column.get('data_type')}} {%- endif -%}
{%- endmacro -%}

{%- macro render_hash_key_lsat_name(models, with_data_type=false) -%}
    {%- set column = models.get('columns') | selectattr("key_type", "equalto", "hash_key_sat") | first -%}
    {{column.get('target')}} {%- if with_data_type %} {{column.get('data_type')}} {%- endif -%}
{%- endmacro -%}

{%- macro render_hash_diff_name(models, with_data_type=false) -%}
    {%- set column = models.get('columns') | selectattr("key_type", "equalto", "hash_diff") | first -%}
    {{column.get('target')}} {%- if with_data_type %} {{column.get('data_type')}} {%- endif -%}
{%- endmacro -%}

{%- macro render_list_biz_key_name(models, with_data_type=false) -%}
    {%- set outs = [] -%}
    {%- for column in models.get('columns') | selectattr("key_type", "equalto", "biz_key") -%}
        {%- set tmp -%}
            {{column.get('target')}} {%- if with_data_type %} {{column.get('data_type')}} {%- endif -%}
        {%- endset -%}
        {% do outs.append(tmp) %}
    {%- endfor -%}
    {{ outs | to_json }}
{%- endmacro -%}

{%- macro render_list_dependent_key_name(models, with_data_type=false) -%}
    {%- set outs = [] -%}
    {%- for column in models.get('columns') | selectattr('key_type', 'equalto', "dependent_key") -%}
        {%- set tmp -%}
            {{column.get('target')}} {%- if with_data_type %} {{column.get('data_type')}} {%- endif -%}
        {%- endset -%}
        {% do outs.append(tmp) %}
    {%- endfor -%}
    {{ outs | to_json }}
{%- endmacro -%}

{%- macro render_list_attr_column_name(models, with_data_type=false) -%}
    {%- set outs = [] -%}
    {%- for column in models.get('columns') | selectattr('key_type', 'undefined') -%}
        {%- set tmp -%}
            {{column.get('target')}} {%- if with_data_type %} {{column.get('data_type')}} {%- endif -%}
        {%- endset -%}
        {% do outs.append(tmp) %}
    {%- endfor -%}
    {{ outs | to_json }}
{%- endmacro -%}

{%- macro render_list_dv_system_column_name(dv_system, with_data_type=false) -%}
    {%- set outs = [] -%}
    {%- for column in dv_system.get('columns') -%}
        {%- set tmp -%}
            {{column.get('target')}} {%- if with_data_type %} {{column.get('data_type')}}{%- endif -%}
        {%- endset -%}
        {% do outs.append(tmp) %}
    {%- endfor -%}
    {{ outs | to_json }}
{%- endmacro -%}

{%- macro render_list_dv_system_ldt_key_name(dv_system) -%}
    {%- set outs = [] -%}
    {%- for key in ('dv_src_ldt', 'dv_kaf_ldt', 'dv_kaf_ofs') -%}
        {%- set tmp = (dv_system.get('columns') | selectattr('target', 'equalto', key) | first).get('target') -%}
        {%- do outs.append(tmp) -%}
    {%- endfor -%}
    {{ outs | to_json }}
{%- endmacro -%}