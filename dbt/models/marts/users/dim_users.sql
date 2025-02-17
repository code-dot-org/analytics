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

state_abbreviations as (
    select * 
    from {{ ref('seed_state_abbreviations') }}
),

final as (
    select 
        -- user info 
        users.user_id,
        users.student_id,
        users.teacher_id,
        users.user_type,
        school_infos.school_id,
        users.studio_person_id,
        users.is_urg,
        users.locale,
        users.birthday,
        users.cap_status,
        users.cap_status_date,
        users_pii.is_ambassador,
        users_pii.teacher_email,
        users_pii.teacher_name,
        users_pii.races,
        users_pii.race_group,
        users.gender,
        users_pii.gender_group,

        -- user geographic info
        users_pii.self_reported_state,
        ug.country,
        sa.state_abbreviation as state,
        ug.us_intl,
        ug.is_international,

        -- aggs 
        users.sign_in_count,
        users.total_lines,     

        -- dates
        users.current_sign_in_at,
        users.last_sign_in_at,
        users.created_at,
        users.updated_at,  
        users.deleted_at,   
        users.purged_at

    from users 
    left join school_infos 
        on users.school_info_id = school_infos.school_info_id
    left join users_pii 
        on users.user_id = users_pii.user_id
    left join user_geos as ug 
        on users.user_id = ug.user_id 
    left join state_abbreviations as sa
        on lower(sa.state_name) = ug.state_name
)

select *
from final 
