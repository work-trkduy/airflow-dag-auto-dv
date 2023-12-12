{%- macro render_target_table_full_name(model) -%}
    {{model.get('target_schema')}}.{{model.get('target')}}
{%- endmacro -%}

{%- macro render_target_der_table_full_name(model) -%}
    {{model.get('target_schema')}}.{{model.get('target') | replace_prefix(model.get('target_entity_type'), model.get('target_entity_type')+'_der')}}
{%- endmacro -%}

{%- macro render_target_snp_table_full_name(model) -%}
    {{model.get('target_schema')}}.{{model.get('target') | replace_prefix(model.get('target_entity_type'), model.get('target_entity_type')+'_snp')}}
{%- endmacro -%}

{%- macro render_target_lsate_table_full_name(model) -%}
    {{model.get('target_schema')}}.lsate_{{model.get('target')}}
{%- endmacro -%}

{%- macro render_source_table_full_name(model) -%}
    {{model.get('source')}}
{%- endmacro -%}

{%- macro render_source_table_view_name(model) -%}
    ${{model.get('source')}}
{%- endmacro -%}

{%- macro render_tblproperties(dv_tblproperties) -%}
    {%- set outs = [] -%}
    {%- for property,value in dv_tblproperties.items() -%}
        {%- set tmp -%}
         '{{property}}' = '{{value|lower}}'
        {% endset -%}
        {%- do outs.append(tmp) -%}
    {%- endfor -%}
    {%- if outs -%}
        tblproperties ({{outs | join(", ")}})
    {%- endif -%}
{%- endmacro -%} 

{%- macro render_tbl_partition(model) -%}
    {%- if "partition" in model -%}
        partitioned by ({{model.get('partition') | join(", ")}})
    {%- else -%}
        partitioned by (days(dv_src_ldt))
    {%- endif -%}
{%- endmacro -%}