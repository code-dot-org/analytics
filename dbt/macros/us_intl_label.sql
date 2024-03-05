{#
    Apply Us vs Intl' label for metrics reports

    In our models, we use id's and keys. Once these data reach the "semantic layer," people don't want to see snake_case, they need clear and technical labels to avoid confusion.

    (js) 2024-02-09
#}

{% macro us_intl_label(is_international) %}

case 
    when {{is_international}} = 1 then 'intl'
    when {{is_international}} = 0 then 'us'
    when {{is_international}} is null then 'missing'
end

{% endmacro %}
