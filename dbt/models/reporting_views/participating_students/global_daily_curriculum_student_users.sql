/* Author: Cory
Date: 11/30/24
Purpose: Used for establishing 2030 participating student goals
This is a daily cut so that the stakeholders can see YOY changes below the school-year level 

Edit log: 


*/

with participating as (
    select * from 
    {{ref('dim_participating_students')}}
    where qualifying_date >= '2019-07-01' --starting with 2019-20 school year
    and country <> 'united states'
    and country is not null
)

, all_students as (
    select 
        student_id,
        school_year,
        min(activity_date) as qualifying_date
    from 
        {{ref('dim_student_script_level_activity')}}
    where 
        user_type = 'student' and
        and country <> 'united states'
        and country is not null
        content_area <> 'hoc' and
        activity_date >= '2019-07-01' --starting with 2019-20 school year
    group by 1,2
)

, participating_counts as (
    select 
    school_year
    , qualifying_date
    , count (distinct case when grade_band = 'HS' then participating.student_id else null end ) n_students_HS
    , count (distinct case when grade_band = 'MS' then participating.student_id else null end ) n_students_MS
    , count (distinct case when grade_band = 'ES' then participating.student_id else null end ) n_students_ES 
    from participating
    group by 
        school_year,
        qualifying_date
)

, all_counts as (
    select
    school_year,
    qualifying_date,
    count (distinct student_id) n_students
    from
        all_students
    group by
        school_year,
        qualifying_date
)

, calculations as (
    select
    participating_counts.school_year as school_year
    , participating_counts.qualifying_date as qualifying_date
    , sum(all_counts.n_students)
        over(
            partition by 
                all_counts.school_year
            order by all_counts.qualifying_date 
            rows between unbounded preceding 
                     and current row) as n_students_cumulative
    , sum(participating_counts.n_students_HS) 
        over(
            partition by 
                participating_counts.school_year
            order by participating_counts.qualifying_date 
            rows between unbounded preceding 
                     and current row) as n_students_hs_cumulative
    , sum(participating_counts.n_students_MS)
        over(
            partition by 
                participating_counts.school_year
            order by participating_counts.qualifying_date 
            rows between unbounded preceding 
                     and current row) as n_students_ms_cumulative
    , sum(participating_counts.n_students_ES)
        over(
            partition by 
                participating_counts.school_year
            order by participating_counts.qualifying_date 
            rows between unbounded preceding 
                     and current row) as n_students_es_cumulative
    from participating_counts
    full outer join all_counts   
        on participating_counts.qualifying_date = all_counts.qualifying_date
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
        (n_students_cumulative + round(0.4 * n_students_es_cumulative))::int as n_students_adj_cumulative,
        n_students_hs_cumulative,
        n_students_ms_cumulative,
        round(n_students_es_cumulative * 1.4)::int as n_students_es_adj_cumulative
    from 
        calculations
)

select * 
from final
order by qualifying_date desc

