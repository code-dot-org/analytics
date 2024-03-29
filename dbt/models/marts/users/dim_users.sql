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
    select *
    from {{ ref('stg_dashboard_pii__users')}}
),

final as (
    select 
        -- users
        users.user_id,
        users.user_type,

        case 
            when users.user_type = 'student' 
                then users.user_id end as student_id,
        case 
            when users.user_type = 'teacher' 
                then users.user_id end as teacher_id,

        users.studio_person_id,
        users.school_info_id,
        users.locale,
        users.sign_in_count,
        users.total_lines,     
        
        users.is_urg,
        users.gender,
        users.birthday,
        
        users_pii.teacher_email,
        users_pii.races,
        users_pii.race_group,
        users_pii.gender_group,

        ug.is_international,
        ug.us_intl,
        ug.country,

        -- dates
        users.current_sign_in_at,
        users.last_sign_in_at,
        users.created_at,
        users.updated_at

    from users 
    left join users_pii 
        on users.user_id = users_pii.user_id
    left join user_geos as ug 
        on users.user_id = ug.user_id
)

select *
from final