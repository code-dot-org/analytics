with 
school_years as (
    select *
    from {{ ref('int_school_years') }}
), 

users as (
    select *
    from {{ ref('stg_dashboard__users') }}
),

user_geos as (
    select *, 
        case when is_international = 1 then 'intl'
             when is_international = 0 then 'us'
             else null end as us_intl
    from {{ ref('stg_dashboard__user_geos') }}
),

users_pii as (
    select *
    from {{ ref('stg_dashboard_pii__users')}}
),

combined as (
    select 
        -- user info
        users.user_id,
        users.user_type,
        users.student_id,
        users.teacher_id,
        users.studio_person_id,
        users.school_info_id,
        users.locale,
        users.total_lines,

        -- user_pii info 
        users_pii.teacher_email,
        users_pii.races,
        users_pii.race_group,
        users_pii.gender_group,
        users_pii.gender,
        users_pii.birthday,
        users_pii.age_years,

        -- user_geo info
        ug.country,
        ug.is_international,
        ug.us_intl,

        -- sysdates
        users.current_sign_in_at,
        users.last_sign_in_at,
        users.created_at,
        users.updated_at,
        users.deleted_at,
        users.purged_at
    from users 
    left join users_pii 
        on users.user_id = users_pii.user_id
    left join user_geos as ug 
        on users.user_id = ug.user_id
    left join school_years as sy 
        on users.created_at 
            between sy.started_at and sy.ended_at)

select *
from combined