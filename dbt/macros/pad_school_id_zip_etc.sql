{#
   # moved to docs
#}

{% macro pad_school_id(school_id) %}
    case
        when length({{ school_id }}) = 11 
        then lpad({{ school_id }},12,0)
        when length({{school_id}}) < 8
        then lpad({{ school_id}},8,0)
        else {{ school_id }}

    end

{% endmacro %}

{% macro pad_ai_code(ai_code) %}
    case
        when length({{ ai_code }}) < 6 
        then lpad({{ ai_code }},6,0)
        else {{ ai_code }}
    end

{% endmacro %}

{% macro pad_zipcode(zip) %}
    case
        when length({{ zip }}) < 5 
        then lpad({{ zip }},5,0)
        else {{ zip }}
    end

{% endmacro %}
