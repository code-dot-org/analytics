{#
    background/description:
    nces school ids should either be 12 characters (public) or 8 characters (private/charter)
    sometimes the leading zeros are dropped in which case we left pad the result.

    POTENTIAL FUTURE SAFE GUARD: 
    - it's theoretically possible for nces ids to have more than 1 leading zero.
    - currently many of the 8-char school ids have as many is 5 leading zeros, for example.
    - none of the 12-char ids to date have more than 1 leading zero, but I'm unsure whether that is hard and fast rule
    - We should EITHER put some kind of test in to ensure that any school_ids are either 8 or 12 characters OR ensure that this macro handles it.

    1.22.24 - however, I'm not baking that safe-guard in here because I haven't researched the true rules of nces id lengths.  So for now, just handling the one known case for length 11
#}

-- Left pad school_id with 0s
{% macro pad_school_id(school_id) %}
    case
        when length({{ school_id }}) = 11 then lpad({{ school_id }}, 12, '0')
        ELSE {{ school_id }}
    END
{% endmacro %}
