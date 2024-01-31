{# Notes:

This macro is an attempt to engage in some DRY practice - to remove repeticious hard-coded labels for
school, teacher, etc. activity.

We have a method for determining whether a school, teacher, section is "active" (see: dim_school_status)  The method produces a
3-digit binary code.  The key is below. This method is used in several locations where a label e.g. "active new"
is applided.

    These 3 values can be combined into an ordered 3-char string representing the concatenated true/false combinations 
    for Active|Prev|Ever e.g. "101" means: ( Active = true AND Prev year = false AND Ever before = true )

    - '000' (0) = 'market'              -- Not active now + never been active
    - '001' (1) = 'inactive churn'      -- NOT active + NOT active prev year + active ever before
    - '010' (2) = '<impossible status>' -- should not be possible, active in the prev year should imply active ever before
    - '011' (3) = 'inactive this year'  -- NOT active + active prev year + (active ever before implied)
    - '100' (4) = 'active new'          -- active this year + NOT active last year + NOT active ever before
    - '101' (5) = 'active reacquired'   -- Active this year + NOT active last year + active in the past
    - '110' (6) = '<impossible status>' -- impossible for same reason as status (2)
    - '111' (7) = 'active retained'     -- active this year + active last year + (active ever before implied) 
#}

{% macro active_status_label(status_code) %}

     case 
            when {{status_code}} = '000' then 'market'
            when {{status_code}}  = '001' then 'inactive churn'
            when {{status_code}}  = '010' then '<impossible status>'
            when {{status_code}}  = '011' then 'inactive this year'
            when {{status_code}}  = '100' then 'active new'
            when {{status_code}}  = '101' then 'active reacquired'
            when {{status_code}}  = '110' then '<impossible status>'
            when {{status_code}}  = '111' then 'active retained'
            when {{status_code}} IS NULL then NULL --on the fence about whether this should pass-thru the null, or whether we should be noisey about it
            else then 'INVALID CODE: ' || {{status_code}}
        end
{% endmacro %}