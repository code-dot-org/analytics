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

school_infos as (
    select * 
    from {{ ref('stg_dashboard__school_infos') }}
),

final as (
    select 
        users.user_id,
        users.student_id,
        users.teacher_id,
        school_infos.school_id,
        users.user_type,
        users.studio_person_id,
        users.is_urg,
        users.gender,
        users.locale,
        users.birthday,
        users.sign_in_count,
        users.total_lines,     
        users.current_sign_in_at,
        users.last_sign_in_at,
        users.created_at,
        users.updated_at,  
        users.deleted_at,   
        users.purged_at,
        users.cap_status,
        users.cap_status_date,
        users_pii.teacher_email,
        users_pii.races,
        users_pii.race_group,
        users_pii.gender_group,
        users_pii.self_reported_state,
        ug.country,
        ug.us_intl,
        ug.is_international

    from users 
    left join school_infos 
        on users.school_info_id = school_infos.school_info_id
    left join users_pii 
        on users.user_id = users_pii.user_id
    left join user_geos as ug 
        on users.user_id = ug.user_id )

select *
from final 
