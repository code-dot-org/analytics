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
        -- user info
        users.user_id,
        users.user_type,
        users.is_urg, 
        users.school_info_id,
        {# users.current_sign_in_at,
        users.last_sign_in_at #}
        users.created_at,
        users.updated_at,
        
        -- geo info 
        user_geos.is_international,
        user_geos.us_intl,
        user_geos.country,
        
        -- pii info 
        users_pii.teacher_email,
        users_pii.races,
        users_pii.race_group,
        users_pii.gender_group,
        users_pii.age_years
    from users 
    left join users_pii 
        on users.user_id = users_pii.user_id
    left join user_geos
        on users.user_id = user_geos.user_id
)

select *
from final