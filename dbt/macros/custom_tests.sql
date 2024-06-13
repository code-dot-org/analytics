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

