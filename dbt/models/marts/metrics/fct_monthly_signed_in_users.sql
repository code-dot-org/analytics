with sign_ins as (
    select *
    from {{ ref('stg_dashboard__sign_ins')}}
),

users as (
    select
        user_id,
        user_type
    from {{ ref('stg_dashboard__users') }} 
    where user_id in (select user_id from sign_ins)
),

user_geos as (
    select
        user_id,
        is_international
    from {{ ref('stg_dashboard__user_geos') }}
    where user_id in (select user_id from users)
),

school_years as (
    select *
    from {{ ref('int_school_years') }}
),

final as (
    select
        u.user_type,
        case when ug.is_international = 1 then 'intl' else 'us' end     as us_intl,
        sy.school_year                                                  as sign_in_school_year,
        date_part(year, si.sign_in_at)::integer                         as sign_in_year,
        date_part(month, si.sign_in_at)::integer                        as sign_in_month,
        count(distinct si.user_id)                                      as num_signed_in_users

    from sign_ins as si
    left join users u 
        on si.user_id = u.user_id
    left join user_geos ug 
        on si.user_id = ug.user_id
    left join school_years sy
        on sign_in_at 
            between sy.started_at and sy.ended_at
    {{ dbt_utils.group_by(5) }}
)

select * 
from final
