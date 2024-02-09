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
        -- user fields
        users.user_id, 
        users.user_type,
        users.is_urg,
        users.school_info_id,

        -- pii fields
        users_pii.teacher_email,
        users_pii.age_years, 
        users_pii.birthday, 
        users_pii.races,
        users_pii.race_group,
        users_pii.gender, 
        users_pii.gender_group,

        -- geo fields
        -- case when ug.is_international = 1 then 'international' else 'united states' end as us_intl,
        ug.country
        ug.is_international,

        -- undecided 
        users.sign_in_count,
        users.total_lines

        -- date fields  
        users.created_at, 
        users.updated_at,
        users.current_sign_in_at, 
        users.last_sign_in_at

    from users 
    left join users_pii 
        on users.user_id = users_pii.user_id
    left join user_geos as ug 
        on users.user_id = ug.user_id
)

select *
from final