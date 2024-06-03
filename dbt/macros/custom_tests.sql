{% test test_string_length_in(model, column_name, list_of_acceptable_lengths) %}

with validation as (
    select len({{ column_name }}) as col_len
    from {{ model }}
),

validation_errors as (
    select *
    from validation
    where col_len not in ({{list_of_acceptable_lengths}})
)

select *
from validation_errors

{% endtest %}

-- macros/custom_tests.sql

{% test test_first_n_columns(model, column_names) %}

    {% set columns = adapter.get_columns_in_relation(ref(model)) %}
    {% set first_n_columns = columns[:column_names | length] %}
    {% set check_columns = first_n_columns | zip(column_names) %}
    
    validation as (
        SELECT 
            column_name AS model_column, 
            expected_column AS expected_column,
            CASE 
                WHEN column_name = expected_column THEN 'match'
                ELSE 'mismatch'
            END AS status
        FROM (
            SELECT 
                column_name, 
                expected_column
            FROM unnest({{ columns }}[:{{ column_names | length }}], {{ column_names }}) AS t (column_name, expected_column)
        )
    ),

    validation_errors as (
        SELECT *
        FROM validation
        WHERE status = 'mismatch'
    )

    SELECT *
    FROM validation_errors

{% endtest %}
