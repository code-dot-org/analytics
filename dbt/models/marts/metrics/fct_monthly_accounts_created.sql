with 
users as (
    select 
        user_id,
        user_type,
        created_at,
        current_sign_in_at
    from {{ref('stg_dashboard__users')}}
    where current_sign_in_at is not null 
),

user_geos as (
    select 
        user_id,
        is_international
    from {{ref('stg_dashboard__user_geos')}}
    where user_id in (select user_id from users)
),

combined as (
    select 
        u.user_id,
        u.created_at,
        u.user_type,
        ug.is_international
    from users u
    left join user_geos ug 
        on ug.user_id = u.user_id
),

school_years as (
    select *
    from {{ ref('int_school_years') }}
),

final as (
    select
        c.user_type,
        case when c.is_international = 1 then 'intl' else 'us' end  as us_intl,
        sy.school_year                                              as created_at_school_year,
        date_part(year, c.created_at)                               as created_at_year,
        date_part(month, c.created_at)                              as created_at_month,
        count(distinct c.user_id)                                   as num_accounts
    from combined as c
    left join school_years as sy 
        on c.created_at 
            between sy.started_at and sy.ended_at
    {{ dbt_utils.group_by(5) }}
)

select * 
from final
