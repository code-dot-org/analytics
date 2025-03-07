/* Author: Cory
Date: 11/30/24
Purpose: Used for establishing 2030 participating student goals
Does all the calculations to generate the school-year level reporting metrics

Edit log: 

Description of qualifying students
- Unique US students with 1+ touchpoint of ES curriculum + 40% uplift
- Unique US students with 1+ touchpoint of MS curriculum
- Unique US students with 5+ touchpoints of CSA/CSP or standalone units for HS
- Total US students = Total unique known students (including 1-4 day HS) + (ES * 40% to account for anonymous)
*/

with participating as (
    select * from 
    {{ref('dim_participating_students')}}
    where qualifying_date >= '2019-07-01' --starting with 2019-20 school year
    where country <> 'united states'
    and country is not null
)

, dssla as (
    select 
        student_id,
        school_year
    from 
        {{ref('dim_student_script_level_activity')}}
    where 
        user_type = 'student' and
        country <> 'united states' and 
        country is not null and
        content_area <> 'hoc' and
        activity_date >= '2019-07-01' --starting with 2019-20 school year
)

, category_counts as (
    select 
    school_year
    , count (distinct case when grade_band = 'HS' then participating.student_id else null end ) n_students_HS
    , count (distinct case when grade_band = 'MS' then participating.student_id else null end ) n_students_MS
    , count (distinct case when grade_band = 'ES' then participating.student_id else null end ) n_students_ES 
    from participating
    group by school_year
)

, total_counts as (
    select
    school_year,
    count (distinct student_id) n_students
    from
        dssla
    group by
        school_year
)

, final as (select
    category_counts.school_year
    , round(total_counts.n_students + n_students_ES * 0.4)::int as n_students_adj
    , n_students_HS
    , n_students_MS
    , round(n_students_ES * 1.4)::int as n_students_ES_adj
    from category_counts
    join total_counts
        on category_counts.school_year = total_counts.school_year
    order by school_year desc
)

select * 
from final