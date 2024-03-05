{%- macro render_target_table_full_name(model) -%}
    {{model.get('target_schema')}}.{{model.get('target_table')}}
{%- endmacro -%}


{%- macro render_target_der_table_full_name(model) -%}
    {{model.get('target_schema')}}.{{model.get('target_table') | replace_prefix(model.get('target_entity_type'), model.get('target_entity_type')+'_der')}}
{%- endmacro -%}


{%- macro render_target_snp_table_full_name(model) -%}
    {{model.get('target_schema')}}.{{model.get('target_table') | replace_prefix(model.get('target_entity_type'), model.get('target_entity_type')+'_snp')}}
{%- endmacro -%}


{%- macro render_target_lsate_table_full_name(model) -%}
    {{model.get('target_schema')}}.{{model.get('target_table') | replace_prefix(model.get('target_entity_type'), 'lsate')}}
{%- endmacro -%}


{%- macro render_source_table_full_name(model) -%}
    {{model.get('source_schema')}}.{{model.get('source_table')}}
{%- endmacro -%}


{%- macro render_source_table_view_name(model) -%}
    ${{model.get('source_schema')}}.{{model.get('source_table')}}
{%- endmacro -%}

{%- macro render_parent_table_full_name(model, target_column=None) -%}
    {%- if column_name == None -%}
        {{model.get('parent_table')}}
    {%- else -%}
        {%- set column = model.get('columns') | selectattr("target", "equalto", target_column) | first -%}
        {%- if column.parent is defined -%}
            {{column.get('parent')}}
        {%- else -%}
            {{model.get('parent_table')}}
        {%- endif -%}
    {%- endif -%}
{%- endmacro -%}

{%- macro render_tblproperties(dv_tblproperties) -%}
{%- if dv_tblproperties -%}

tblproperties (
    {%- for property,value in dv_tblproperties.items() %}
    '{{property}}' = '{{value|lower}}' {{- ', ' if not loop.last -}}
    {%- endfor %}
)
    
{%- endif -%}
{%- endmacro -%} 


{%- macro render_tbl_partition(model) -%}
    {%- if "partition" in model -%}
        partitioned by ({{model.get('partition') | join(", ")}})
    {%- else -%}
        partitioned by (days(dv_src_ldt))
    {%- endif -%}
{%- endmacro -%}