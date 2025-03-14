{% macro get_email_domain(email)%}
     case 
        when {{email}} is not null and len({{email}}) > 0
            then 
            right(
                {{ email }}, 
                strpos(reverse({{ email }}), '@')-1 
            ) 
        else
            null
        end as email_domain
{% endmacro%}


