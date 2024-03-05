{%- from 'models/test/test_unique.sql' import render_list_column_unique_tests with context -%}
{%- from 'models/test/test_not-null.sql' import render_list_column_not_null_tests with context -%}
{%- from 'models/test/test_orphan.sql' import render_list_column_orphan_tests with context -%}

{%- set test_queries = [] -%}
{%- do test_queries.extend(render_list_column_unique_tests(model) | from_json) -%}
{%- do test_queries.extend(render_list_column_not_null_tests(model) | from_json) -%}
{%- do test_queries.extend(render_list_column_orphan_tests(model) | from_json) -%}

{{test_queries | join('\nunion all\n')}}