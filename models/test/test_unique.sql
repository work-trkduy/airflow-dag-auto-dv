{%- from "macros/extract_name_tables.sql" import
    render_target_table_full_name -%}

{%- macro _render_column_unique_test(target_table, target_column) -%}
select
    cast({{target_column}} as string) as error_value,
    '{{target_column}}' as error_column,
    'unique' as error_code
from {{target_table}}
group by {{target_column}}
having count(1) > 1
{%- endmacro -%}

{%- macro render_list_column_unique_tests(model) -%}
    {%- set test_queries = [] -%}
    {%- for column in model.get('columns') | selectattr("tests", "defined") -%}
        {%- if (column.get('tests') | select("equalto", "unique") | list | length) > 0 -%}
            {%- set target_table = render_target_table_full_name(model) -%}
            {%- set target_column = column.get('target') -%}
            {%- do test_queries.append(_render_column_unique_test(target_table, target_column)) -%}
        {%- endif -%}
    {%- endfor -%}
    {{test_queries | to_json}}
{%- endmacro -%}

{{render_list_column_unique_tests(model) | from_json | join('\nunion all\n')}}