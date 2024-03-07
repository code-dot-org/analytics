{{ 
    config(
        materialized='incremental',
        unique_key='user_id')
}}

with
users as (
    select *
    from {{ ref('base_dashboard_pii__users') }}
    where is_active
        and user_type is not null 

    {% if is_incremental() %}

    and updated_at > (select max(updated_at) from {{ this }} )
    
    {% endif %}
),

renamed as (
    select
        user_id,
        user_type,
        case when user_type = 'student' then user_id end as student_id,
        case when user_type = 'teacher' then user_id end as teacher_id,
        case when user_type = 'teacher' then email else null end as teacher_email, -- PII!
        birthday,
        datediff(year, birthday, current_date) as age_years,
        is_active,
        is_urg,
        races,
        case 
            when races like '%hispanic%' then 'hispanic'

            -- If races contains a comma AND is_urg return 'two or more urg'
            when races like '%,%' and is_urg = 1 then 'two or more urg'
            
            -- If races contains a comma and not caught by case above, then urg is 0 or null, return 'two or more non urg'
            when races like '%,%' then 'two or more'
            
            -- If races matches any of these specific strings, return 'no_response'
            -- when
            --     races in ('closed_dialog', 'nonsense', 'opt_out')
            --     then 'n/a'
            -- If races is NULL, return NULL
            -- when races is null then 'n/a'
        
            -- Default case: return the input value
            -- else races -- Additional logic may be required here
        
            else null
        end as race_group,
        
        nullif(lower(gender), '') as gender,
        case 
            when lower(gender) in ('m','f','n','o') 
            then lower(gender)
            else null 
        end as gender_group,

        created_at,
        updated_at,
        purged_at,
        deleted_at
    from users)

select *
from renamed