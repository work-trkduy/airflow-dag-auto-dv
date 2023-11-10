{%- macro render_target_table_full_name(target_schema, models) -%}
    {{target_schema}}.{{models.get('target')}}
{%- endmacro -%}

{%- macro render_target_der_table_full_name(target_schema, models, target_type) -%}
    {{target_schema}}.{{models.get('target') | replace_prefix(target_type, target_type+'_der')}}
{%- endmacro -%}

{%- macro render_target_snp_table_full_name(target_schema, models, target_type) -%}
    {{target_schema}}.{{models.get('target') | replace_prefix(target_type, target_type+'_snp')}}
{%- endmacro -%}

{%- macro render_target_lsate_table_full_name(target_schema, models) -%}
    {{target_schema}}.lsate_{{models.get('target')}}
{%- endmacro -%}

{%- macro render_source_table_full_name(models) -%}
    {{models.get('source')}}
{%- endmacro -%}

{%- macro render_source_table_view_name(models) -%}
    ${{models.get('source')}}
{%- endmacro -%}

{%- macro render_batch_job_log_table_full_name() -%}
    auto_dv_metadata.logging_etl_batch_job
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

{%- macro render_tbl_partition(models) -%}
    {%- if "partition" in models -%}
        partitioned by ({{models.get('partition') | join(", ")}})
    {%- else -%}
        partitioned by (days(dv_src_ldt))
    {%- endif -%}
{%- endmacro -%}