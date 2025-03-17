/* Author: Cory
Date: 11/30/24
Purpose: Used for establishing 2030 participating student goals
This is a daily cut so that the stakeholders can see YOY changes below the school-year level 


Edit log: 

2025-03-17 CK - edited to make global

*/

with curriculum_students as (
    select * from 
    {{ref('dim_curriculum_student_users')}}
    where qualifying_date >= '2020-07-01' --starting with 2020-21 school year
)

--all_students required here because the total includes 1+ days of HS and that's not in dim_curriculum_student_users
, all_students as (
    select 
        student_id,
        school_year,
        us_intl,
        country,
        min(activity_date) as qualifying_date
    from 
        {{ref('dim_student_script_level_activity')}}
    where 
        user_type = 'student' and
        content_area <> 'hoc' and
        activity_date >= '2020-07-01' --starting with 2020-21 school year
    group by 1,2,3,4
)

, curriculum_counts as (
    select 
    school_year
    , qualifying_date
    , us_intl
    , country
    , count (distinct case when grade_band = 'HS' then curriculum_students.student_id else null end ) n_students_HS
    , count (distinct case when grade_band = 'MS' then curriculum_students.student_id else null end ) n_students_MS
    , count (distinct case when grade_band = 'ES' then curriculum_students.student_id else null end ) n_students_ES 
    from curriculum_students
    group by 1,2,3,4
)

, all_counts as (
    select
    school_year,
    qualifying_date,
    country,
    us_intl,
    count (distinct student_id) n_students
    from
        all_students
    group by 1,2,3,4
)

, calculations as (
    select
    curriculum_counts.school_year as school_year
    , curriculum_counts.qualifying_date as qualifying_date
    , curriculum_counts.us_intl
    , curriculum_counts.country
    , sum(all_counts.n_students)
        over(
            partition by 
                all_counts.school_year, all_counts.country
            order by all_counts.qualifying_date 
            rows between unbounded preceding 
                     and current row) as n_students
    , sum(curriculum_counts.n_students_HS) 
        over(
            partition by 
                curriculum_counts.school_year, curriculum_counts.country
            order by curriculum_counts.qualifying_date 
            rows between unbounded preceding 
                     and current row) as n_students_hs
    , sum(curriculum_counts.n_students_MS)
        over(
            partition by 
                curriculum_counts.school_year, curriculum_counts.country
            order by curriculum_counts.qualifying_date 
            rows between unbounded preceding 
                     and current row) as n_students_ms
    , sum(curriculum_counts.n_students_ES)
        over(
            partition by 
                curriculum_counts.school_year, curriculum_counts.country
            order by curriculum_counts.qualifying_date 
            rows between unbounded preceding 
                     and current row) as n_students_es
    from curriculum_counts
    full outer join all_counts   
        on curriculum_counts.qualifying_date = all_counts.qualifying_date
        and curriculum_counts.country = all_counts.country
    where curriculum_counts.qualifying_date is not null
)

, final as (
    select 
        school_year,
        qualifying_date,
        date_part(week, qualifying_date)::int week_number,
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
order by qualifying_date desc, us_intl

    select
        school_year,
        qualifying_date,
        us_intl,
        /*decode (date_part(dayofweek, qualifying_date),
                     0, 'sun',
                     1, 'mon',
                     2, 'tue',
                     3, 'wed',
                     4, 'thu',
                     5, 'fri',
                     6, 'sat')
        day_of_the_week,

    from 
        calculations
)



