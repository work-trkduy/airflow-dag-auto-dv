{%- from "macros/extract_name_tables.sql" import
    render_target_table_full_name -%}

{%- macro _render_column_orphan_test(target_column, parent_table, parent_column) -%}
select
    cast({{target_column}} as string) as error_value,
    '{{target_column}}' as error_column,
    'orphan' as error_code
from {{render_target_table_full_name(model)}} a
where not exists (
    select 1 from {{parent_table}} b
    where a.{{target_column}} = b.{{parent_column}}
)
{%- endmacro -%}

{%- macro render_list_column_orphan_tests() -%}
    {%- set test_queries = [] -%}
    {%- for column in model.get('columns') | selectattr("tests", "defined") -%}
        {%- for test in column.get('tests') | selectattr("orphan", "defined") -%}
            {%- set target_table = model.get('target') -%}
            {%- set target_column = column.get('target') -%}
            {%- set parent_table = test.get("orphan").get('parent_table') -%}
            {%- set parent_column = test.get("orphan").get('parent_column') -%}
            {%- do test_queries.append(_render_column_orphan_test(target_column, parent_table, parent_column)) -%}
        {%- endfor -%}
    {%- endfor -%}
    {{test_queries | to_json}}
{%- endmacro -%}

{{render_list_column_unique_tests() | from_json | join('\nunion all\n')}}