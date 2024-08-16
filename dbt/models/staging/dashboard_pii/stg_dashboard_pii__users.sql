with
users as (
    select *
    from {{ ref('base_dashboard_pii__users') }}
    where is_active
        and user_type is not null 
),

state_abbreviations as (
    select distinct
        lower(state_abbreviation)   as us_state_abbr,
        lower(state_name)           as us_state_name,
        1                           as is_us_state
    from {{ ref('seed_state_abbreviations') }}
),

combined as (
    select 
        usr.* , 
        case 
        
            when    sta.us_state_name is not null 
                and sta.is_us_state = 1
            then sta.us_state_abbr -- always return varchar(2) value 

            when    sta.us_state_abbr is not null 
                and sta.is_us_state = 1
            then sta.us_state_abbr 
            
            else null end as us_state_abbr -- if not one of the US States, then null 

    from users                      as usr
    left join state_abbreviations   as sta 
        on usr.us_state = 
            coalesce(
                sta.us_state_name,
                sta.us_state_abbr ) -- if us_state matches a us state, return the abbreviate (varchar (2) )
    ),

renamed as (
    select
        user_id,
        name,
        user_type,
        case when user_type = 'student' then user_id end            as student_id,
        
        case when user_type = 'teacher' then user_id end            as teacher_id,
        case when user_type = 'teacher' then email else null end    as teacher_email, -- PII!
        
        birthday,
        datediff(year, birthday, current_date)  as age_years,
        races,
        us_state_abbr                           as self_reported_state, -- entered originally by user

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
            else races 
            -- Additional logic may be required here
        end                                     as race_group,

        nullif(lower(gender), '')               as gender,
        
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

    from combined )

select *
from renamed