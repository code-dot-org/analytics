{{
    config(
        materialized='incremental',
        unique_key='user_id')
}}

with 
users as (
    select *
    from {{ ref('base_dashboard__users') }}
    where is_active

    {% if is_incremental() %}

    and updated_at > (select max(updated_at) from {{ this }} )
    
    {% endif %}
),

renamed as (
    select 
        -- PK
        user_id,

        -- FK's
        case when user_type = 'student' then user_id end as student_id,
        case when user_type = 'teacher' then user_id end as teacher_id,
        studio_person_id,

        -- user demogrphic info
        user_type,
        birthday,
        datediff(year,birthday,current_date ) as age_years_today,
        --nullif(lower(gender),'') as gender, 

        -- logic for gender code
        case 
            when gender = 'm' then 'm'
            when gender = 'f' then 'f'
            when gender = 'n' then 'nb' ------  non-binary
            when gender = 'o' then 'nl' ------  not listed
            when gender = '' OR gender = '-' then 'no_response' ----  empty string or '-'
            else null
        end as gender,
    
        -- logic for making single-race/ethnicity designation. order of operations matters.
        case 
            -- If races matches any of these specific strings, return 'no_response'
            -- These are all versions of user choosing not to respond and is distinct from NULL
            when races IN ('closed_dialog', 'nonsense', 'opt_out') THEN 'no_response'
            
            -- hispanic with anything else = hispanic
            when races LIKE '%hispanic%' THEN 'hispanic'

            -- If races contains a comma, return 'tr' (_T_wo or more _R_aces), a common abbreviation
            when races LIKE '%,%' THEN 'tr'

            -- If races is NULL, return NULL.  NULL is different from no_response
            when races IS NULL THEN NULL

            -- Default case: return the input value
            else races -- should be a single race. Additional logic may be required here in the future
        END race,

        is_urg,

        -- misc.
        locale,
        sign_in_count,
        school_info_id,
        total_lines,

        -- dates         
        current_sign_in_at,
        last_sign_in_at,
        created_at,
        updated_at,     
        purged_at
    from users
)

select * 
from renamed
