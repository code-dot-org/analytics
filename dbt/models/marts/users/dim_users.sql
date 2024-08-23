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
        users.*, 
        users_pii.teacher_email,
        users_pii.races,
        users_pii.race_group,
        users_pii.gender_group,
        users_pii.self_reported_state,
        ug.country,
        ug.us_intl,
        ug.is_international

    from users 
    left join users_pii 
        on users.user_id = users_pii.user_id
    left join user_geos as ug 
        on users.user_id = ug.user_id )

select *
from final 