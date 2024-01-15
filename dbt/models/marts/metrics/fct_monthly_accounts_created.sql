{# NOTES:
accounts created by month - user account was created in the given month (and year)
segmented by student/
EXCLUDING "dummy accounts" - student accounts created but don't have a sign_in attempt.
use case: teacher creates a section of picture accounts many of which don't get used.
#}
-- with student_accounts as (
--     select
--         student_id,
--         user_type,
--         is_international,
--         created_at,
--         created_at_school_year
--     from {{ ref('dim_students') }}
--     where current_sign_in_at is not NULL
-- ),

-- teacher_accounts as (

--     select
--         teacher_id,
--         user_type,
--         is_international,
--         created_at,
--         created_at_school_year
--     from {{ ref('dim_teachers') }}
--     where current_sign_in_at is not NULL -- don't think NULL is possible for a teacher account, but doing for sense of completion
-- ),

-- all_accounts as (
--     select
--         student_id as user_id,
--         user_type,
--         is_international,
--         created_at,
--         created_at_school_year
--     from student_accounts
--     union all
--     select * from teacher_accounts
-- )

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
,all_users_and_geos as (
    select u.*, ug.is_international
    from all_users u
    left join user_geos ug on ug.user_id = u.user_id
)
select
    count(distinct user_id) as num_accounts_created,
    date_part(month, created_at) as account_created_month,
    date_part(year, created_at) as account_created_year,
    created_at_school_year,
    user_type,
    case when is_international = 1 then 'intl' else 'us' end as us_intl
from all_accounts
group by 2, 3, 4, 5, 6
order by
    account_created_year asc,
    account_created_month asc,
    user_type asc,
    us_intl desc
