{#
   # moved to docs
#}

{% macro pad_school_id(school_id) %}
    case
        when length({{ school_id }}) = 11 
        then lpad({{ school_id }},12,0)
        else {{ school_id }}
    end

{% endmacro %}
