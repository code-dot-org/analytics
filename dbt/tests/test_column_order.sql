/*
Would like to use this on a few instances for source/base AP tables where we want to ensure
the existence and order of the first n columns so that unpivoting can work.

But I couldn't get it to work due to my lack of DBT experience - I  was unsure of where/how to use a test like this and moved on.
*/

{% test test_column_order(expected_columns) %}
  {%- set expected_columns_cte -%}
    {%- for col in expected_columns -%}
        select '{{ col }}' as column_name, {{ loop.index }} as ordinal_position
        {%- if not loop.last -%}
            union all
        {%- endif -%}
    {%- endfor -%}
  {%- endset -%}

  with actual_columns as (
      select column_name, ordinal_position
      from information_schema.columns
      where table_schema = '{{ this.schema }}' and table_name = '{{ this.table }}'
  ),
  expected_columns as (
      {{ expected_columns_cte }}
  )
  select
      actual_columns.column_name as actual_column,
      actual_columns.ordinal_position as actual_position,
      expected_columns.column_name as expected_column,
      expected_columns.ordinal_position as expected_position
  from
      actual_columns
      full outer join expected_columns
      on actual_columns.column_name = expected_columns.column_name
      and actual_columns.ordinal_position = expected_columns.ordinal_position
  where
      actual_columns.column_name is null
      or expected_columns.column_name is null
      or actual_columns.ordinal_position is null
      or expected_columns.ordinal_position is null
{% endtest %}
