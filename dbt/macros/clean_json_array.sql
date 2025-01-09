{% macro clean_json_array(column_name) %}
    replace(
        replace(
            replace(
                {{ column_name }},
                '[', ''
            ),
            ']', ''
        ),
        '"', ''
    )
{% endmacro %}