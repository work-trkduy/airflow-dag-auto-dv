{%- from "macros/extract_name_tables.sql" import
    render_target_table_full_name,
    render_parent_table_full_name -%}

{%- macro _render_column_orphan_test(target_table, parent_table, target_column) -%}
select
    cast({{target_column}} as string) as error_value,
    '{{target_column}}' as error_column,
    'orphan' as error_code
from {{target_table}} a
where not exists (
    select 1 from {{parent_table}} b
    where a.{{target_column}} = b.{{target_column}}
)
{%- endmacro -%}

{%- macro render_list_column_orphan_tests(model) -%}
    {%- set test_queries = [] -%}
    {%- for column in model.get('columns') | selectattr("tests", "defined") -%}
        {%- if (column.get('tests') | select("equalto", "orphan") | list | length) > 0 -%}
            {%- set target_table = render_target_table_full_name(model) -%}
            {%- set target_column = column.get('target') -%}
            {%- set parent_table = render_parent_table_full_name(model, target_column) -%}
            {%- do test_queries.append(_render_column_orphan_test(target_table, parent_table, target_column)) -%}
        {%- endif -%}
    {%- endfor -%}
    {{test_queries | to_json}}
{%- endmacro -%}

{{render_list_column_unique_tests(model) | from_json | join('\nunion all\n')}}