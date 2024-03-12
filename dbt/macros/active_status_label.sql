{% macro active_status_label(status_code) %}

    case 
        when {{status_code}}    = 000 then 'market'
        when {{status_code}}    = 001 then 'inactive churn'
        when {{status_code}}    = 010 then '<impossible status>' -- revisit some of these labels (js)
        when {{status_code}}    = 011 then 'inactive this year'
        when {{status_code}}    = 100 then 'active new'
        when {{status_code}}    = 101 then 'active reacquired'
        when {{status_code}}    = 110 then '<impossible status>'
        when {{status_code}}    = 111 then 'active retained'
        else then 'invalid code: ' || {{status_code}}
    end
{% endmacro %}