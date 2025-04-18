/* Author: Cory
Date: 11/30/24
Purpose: Used for establishing 2030 participating student goals
This is a daily cut so that the stakeholders can see YOY changes below the school-year level 


Edit log: 

2025-03-17 CK - edited to make global + replaced dssla with duca as a source of all_students data (for speed)
2025-04-17 CK - edited to add school year week

*/

with curriculum_counts as (
    select 
        school_year
        , qualifying_date
        , us_intl
        , country
        , count (distinct case when grade_band = 'HS' then student_id else null end ) n_students_hs
        , count (distinct case when grade_band = 'MS' then student_id else null end ) n_students_ms
        , count (distinct case when grade_band = 'ES' then student_id else null end ) n_students_es
    from {{ref('dim_curriculum_student_users')}}
    where qualifying_date >= '2020-07-01' --replace with dynamic reference
    group by 1,2,3,4  
)

--duca used here because the total includes 1+ days of HS and that's not in dim_curriculum_student_users
, all_students as (
    select 
        user_id,
        school_year,
        us_intl,
        country,
        min({{ date_trunc("day", "first_activity_at") }}) as qualifying_date
    from 
        {{ref('dim_user_course_activity')}}
    where 
        user_type = 'student' and
        content_area like '%curriculum%' and
        first_activity_at >= '2020-07-01' and
        country is not null
    group by 1,2,3,4
)

, student_aggregate as (
    select 
        qualifying_date,
        school_year,
        us_intl,
        country,
        count(distinct user_id) as n_students
    from 
        all_students
    group by 1,2,3,4
)

, countries as (
    select country, us_intl
    from student_aggregate
    group by 1,2
)

, school_weeks as (
    select * 
    from {{ ref('int_school_weeks') }}
)

, date_spine as (
    {{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2020-07-01' as date)",
    end_date="sysdate"
   )
}}
)

, school_years as (
    select * 
    from {{ ref('int_school_years') }}
)

, frame as (
    select *
    from date_spine
    cross join countries
)

, frame_with_data as (
    select
        frame.date_day as qualifying_date
        , frame.country
        , frame.us_intl
        , school_years.school_year
        , coalesce(student_aggregate.n_students, 0) as n_students
        , coalesce(curriculum_counts.n_students_hs, 0) as n_students_hs
        , coalesce(curriculum_counts.n_students_ms, 0) as n_students_ms
        , coalesce(curriculum_counts.n_students_es, 0) as n_students_es
    from frame
    left join curriculum_counts 
        on curriculum_counts.qualifying_date = frame.date_day
        and curriculum_counts.country = frame.country
    left join student_aggregate 
        on student_aggregate.qualifying_date = frame.date_day
        and student_aggregate.country = frame.country
    inner join school_years 
        on frame.date_day
            between school_years.started_at 
                and school_years.ended_at
)

, calculations as (
    select
    qualifying_date
    , school_year
    , us_intl
    , country
    , sum(n_students) 
        over(
            partition by 
                school_year, country
            order by qualifying_date 
            rows between unbounded preceding 
                     and current row) as n_students
    , sum(n_students_hs) 
        over(
            partition by 
                school_year, country
            order by qualifying_date 
            rows between unbounded preceding 
                     and current row) as n_students_hs
    , sum(n_students_ms) 
        over(
            partition by 
                school_year, country
            order by qualifying_date 
            rows between unbounded preceding 
                     and current row) as n_students_ms
    , sum(n_students_es) 
        over(
            partition by 
                school_year, country
            order by qualifying_date 
            rows between unbounded preceding 
                     and current row) as n_students_es
    from frame_with_data
)

, final as (
    select 
        calculations.school_year
        , qualifying_date
        , date_part(week, qualifying_date)::int week_number
        , sw.week_number_school_year
        , decode (date_part(dayofweek, qualifying_date),
                     0, 'sun',
                     1, 'mon',
                     2, 'tue',
                     3, 'wed',
                     4, 'thu',
                     5, 'fri',
                     6, 'sat') as day_of_the_week
        , us_intl
        , country
        , coalesce(n_students, 0) as n_students
        , coalesce((n_students + round(0.4 * n_students_es)::int),0) as n_students_adj
        , coalesce(n_students_hs, 0) as n_students_hs
        , coalesce(n_students_ms, 0) as n_students_ms
        , coalesce(n_students_es, 0) as n_students_es
        , coalesce(round(n_students_es * 1.4)::int,0) as n_students_es_adj
    from calculations
    left join school_weeks  as sw
        on calculations.qualifying_date 
            between sw.started_at 
            and sw.ended_at
)

select * 
from final
order by qualifying_date, country