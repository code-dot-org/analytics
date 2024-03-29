with
users as (
    select *
    from {{ ref('base_dashboard_pii__users') }}
    where is_active
        and user_type is not null 
),

renamed as (
    select
        user_id,
        user_type,

        case 
            when user_type = 'teacher' 
                then email 
            else null 
        end as teacher_email, -- PII!
        
        birthday,
        datediff(year, birthday, current_date) as age_years,
        races,
        case
            -- If races contains 'hispanic', return 'hispanic'
            when races like '%hispanic%' then 'hispanic'
            
            -- If races matches any of these specific strings, return 'no_response'
            when
                races in ('closed_dialog', 'nonsense', 'opt_out')
                then 'no_response'
            
            -- If races contains a comma AND is_urg return 'two or more urg'
            when races like '%,%' and is_urg = 1 then 'two_or_more_urg'
            
            -- If races contains a comma and not caught by case above, then urg is 0 or null, return 'two or more non urg'
            when races like '%,%' then 'two_or_more_non_urg'
            
            -- If races is NULL, return NULL
            when races is null then 'not_collected'
            
            -- Default case: return the input value
            else races -- Additional logic may be required here
        end as race_group,

        nullif(lower(gender), '') as gender,
        case
            when gender = 'm' then 'm'
            when gender = 'f' then 'f'
            when gender = 'n' then 'nb' -- non-binary
            when gender = 'o' then 'nl' -- not listed
            when gender = '' or gender = '-' 
                then 'no_response' -- empty string or '-'

            when gender is null or gender in (
                'k', 'd', 'x', 'v', 'b', 'a', 'A')  --these all have a single user associated with them.
                then 'not_collected'
                
            else 'unexpected value: ' || gender
        end as gender_group
    from users
)

select *
from renamed