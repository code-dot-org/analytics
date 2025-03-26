/* Author: Cory
Date: 11/30/24
Purpose: Used for establishing 2030 participating student goals
This is a daily cut so that the stakeholders can see YOY changes below the school-year level 


Edit log: 

2025-03-17 CK - edited to make global

*/

with curriculum_counts as (
    select 
        school_year
        , qualifying_date
        , us_intl
        , country
        , count (distinct case when grade_band = 'HS' then student_id else null end ) n_students_HS
        , count (distinct case when grade_band = 'MS' then student_id else null end ) n_students_MS
        , count (distinct case when grade_band = 'ES' then student_id else null end ) n_students_ES 
    from {{ref('dim_curriculum_student_users')}}
    where qualifying_date >= '2020-07-01' --replace with dynamic reference
    group by 1,2,3,4  
)

--duca used here because the total includes 1+ days of HS and that's not in dim_curriculum_student_users
, all_counts as (
    select 
       {{ date_trunc("day", "first_activity_at") }} as qualifying_date,
        school_year,
        us_intl,
        country,
        count(distinct user_id) as n_students
    from 
        {{ref('dim_user_course_activity')}}
    where 
        user_type = 'student' and
        content_area like '%curriculum%' and
        first_activity_at >= '2020-07-01'
    group by 1,2,3,4
)

, countries as (
    select country, us_intl
    from all_counts
    group by 1,2
)

, date_spine as (
  {{dbt_utils.date_spine(
    datepart="day",
    start_date= "to_date('2020-07-01', 'yyyy-mm-dd')",
    end_date= "to_date('2025-07-01', 'yyyy-mm-dd')"
    )}}
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

, calculations as (
    select
    frame.date_day as qualifying_date
    , school_years.school_year
    , frame.us_intl
    , frame.country
    , coalesce(sum(all_counts.n_students) 
        over(
            partition by 
                all_counts.school_year, all_counts.country
            order by all_counts.qualifying_date 
            rows between unbounded preceding 
                     and current row),0) as n_students
    , coalesce(sum(curriculum_counts.n_students_HS) 
        over(
            partition by 
                curriculum_counts.school_year, curriculum_counts.country
            order by curriculum_counts.qualifying_date 
            rows between unbounded preceding 
                     and current row),0) as n_students_hs
    , coalesce(sum(curriculum_counts.n_students_MS)
        over(
            partition by 
                curriculum_counts.school_year, curriculum_counts.country
            order by curriculum_counts.qualifying_date 
            rows between unbounded preceding 
                     and current row),0) as n_students_ms
    , coalesce(sum(curriculum_counts.n_students_ES)
        over(
            partition by 
                curriculum_counts.school_year, curriculum_counts.country
            order by curriculum_counts.qualifying_date 
            rows between unbounded preceding 
                     and current row),0) as n_students_es
    from frame
    left join curriculum_counts 
        on curriculum_counts.qualifying_date = frame.date_day
        and curriculum_counts.country = frame.country
    left join all_counts 
        on all_counts.qualifying_date = frame.date_day
        and all_counts.country = frame.country
    inner join school_years 
        on frame.date_day
            between school_years.started_at 
                and school_years.ended_at
)

, final as (
    select 
        school_year,
        qualifying_date,
        date_part(week, qualifying_date)::int week_number,
        decode (date_part(dayofweek, qualifying_date),
                     0, 'sun',
                     1, 'mon',
                     2, 'tue',
                     3, 'wed',
                     4, 'thu',
                     5, 'fri',
                     6, 'sat')
        day_of_the_week,
        us_intl,
        country,
        n_students,
        (n_students + round(0.4 * n_students_es))::int as n_students_adj,
        n_students_hs,
        n_students_ms,
        n_students_es,
        round(n_students_es * 1.4)::int as n_students_es_adj
    from calculations
)

select * 
from final
order by qualifying_date, country