{# NOTES:
accounts created by month - user account was created in the given month (and year)
segmented by student/
EXCLUDING "dummy accounts" - student accounts created but don't have a sign_in attempt.
use case: teacher creates a section of picture accounts many of which don't get used.

NOTE: right now delted accounts are filtered out at the base_users table.  Waiting on a change
to that in order to make these counts accurate.
#}

with all_users as (
    select 
        user_id,
        user_type,
        created_at,
        current_sign_in_at
    from {{ref('stg_dashboard__users')}}
    where current_sign_in_at is not NULL -- don't count "dummy accounts"
)
, user_geos as (
    select 
        user_id,
        is_international
    from {{ref('stg_dashboard__user_geos')}}
)
,all_accounts as (
    select u.*, ug.is_international
    from all_users u
    left join user_geos ug on ug.user_id = u.user_id
)
, school_years as (
    select *
    from {{ ref('int_school_years') }}
)
select
    count(distinct user_id) as num_accounts,
    date_part(month, created_at) as created_at_month,
    date_part(year, created_at) as created_at_year,
    sy.school_year as created_at_school_year,
    user_type,
    case when is_international = 1 then 'intl' else 'us' end as us_intl
from all_accounts ac
left join school_years sy on ac.created_at::date between sy.started_at and sy.ended_at
group by 2, 3, 4, 5, 6
order by
    account_created_year asc,
    account_created_month asc,
    user_type asc,
    us_intl desc
