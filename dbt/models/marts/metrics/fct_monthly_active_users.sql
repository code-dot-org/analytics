{# Notes:
def: monthly active user -- any user who has signed in at least once during a given month (and year)
segmented by student/teacher and us/intl

I tried making this joining to dim_students and dim_teachers to get is_international flag but the result
was that hundreds of thousands of students and teachers with signins are not included in those tables
...probably for a good reason, but I can't use it to make monthly signins apples to apples with old metrics.
is_international is computed in the user_geos table anyway so I'm not recreating that logic here, which was the real fear.

I've left my original attempt commented out below.
#}
with sign_ins as (
    select *
    from {{ ref('base_dashboard__sign_ins') }} -- replace with stg_ eventually?
),

all_users as (
    select
        user_id,
        user_type
    from {{ ref('stg_dashboard__users') }} --WARNING this has already filtered out deleted users
    
),

user_geos as (
    select
        user_id,
        is_international
    from {{ ref('stg_dashboard__user_geos') }}
),


-- student_geos as (
--     select
--         student_id,
--         user_type,
--         is_international
--     from {{ ref('dim_students') }}
-- ),

-- teacher_geos as (
--     select
--         teacher_id,
--         user_type,
--         is_international
--     from {{ ref('dim_teachers') }}
-- ),

-- -- open question if this should be split into student and teacher active user metrics (or marts) tables
-- -- I'm unioning them here for convience of reporting.
-- all_user_geos as (
--     select
--         student_id as user_id,
--         user_type,
--         is_international
--     from student_geos
--     union all
--     select * from teacher_geos
-- ),

school_years as (
    select *
    from {{ ref('int_school_years') }}
)

select
    count(distinct si.user_id) as num_users,
    date_part(month, si.sign_in_at) as sign_in_month,
    date_part(year, si.sign_in_at) as sign_in_year,
    --  not relevant for international but including for convienence
    sy.school_year,
    user_type,
    case when is_international = 1 then 'intl' else 'us' end as us_intl

from sign_ins as si
join all_users u on si.user_id = u.user_id --inner join to preserve filterd out deleted users
left join user_geos ug on si.user_id = ug.user_id
left join
    school_years as sy
    on sign_in_at::date between sy.started_at and sy.ended_at
group by 2, 3, 4, 5, 6
