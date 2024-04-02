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
        users.*, 
        users_pii.teacher_email,
        ug.is_international,
        ug.us_intl,
        ug.country,
        sch.school_id
    from users 
    left join users_pii 
        on users.user_id = users_pii.user_id
    left join user_geos as ug 
        on users.user_id = ug.user_id
    left join school_infos as sch 
        on users.school_info_id = sch.school_info_id )

select *
from final