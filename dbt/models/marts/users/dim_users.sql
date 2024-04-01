with 
users as (
    select * 
    from {{ ref('stg_dashboard__users') }}
),

user_geos as (
    select * 
    from {{ ref('stg_dashboard__user_geos') }}
),

users_pii as (
    select 
        user_id,
        teacher_email
    from {{ ref('stg_dashboard_pii__users')}}
    where user_type = 'teacher'
),

combined as (
    select 
        -- users
        users.*,
        
        -- user geos 
        ug.country,
        ug.is_international,
        ug.us_intl,
        
        -- PII!!!
        usp.teacher_email

    from users 
    left join users_pii as usp  
        on users.user_id = usp.user_id
    left join user_geos as ug 
        on users.user_id = ug.user_id
),

final as (
    select 
        -- keys 
        user_id,
        student_id,
        teacher_id,
        studio_person_id,
        school_info_id,
        
        -- user info
        user_type,
        locale,
        is_urg,    
        birthday,
        age_years,
        races,
        race_group,
        gender,
        gender_group,

        -- users pii
        teacher_email,
        
        -- counts
        sign_in_count,
        total_lines,     

        -- user geos
        country,
        is_international,
        us_intl,
        
        -- sysdates
        current_sign_in_at,
        last_sign_in_at,
        created_at,
        updated_at,  
        purged_at,
        deleted_at
    from combined )

select *
from final