/* Author: Cory
Date: 11/30/24
Purpose: Used for establishing 2030 participating student goals
Does all the calculations to generate the school-year level reporting metrics

Edit log: 

Description of qualifying students
- Unique US students with 1+ touchpoint of ES curriculum + 40% uplift
- Unique US students with 1+ touchpoint of MS curriculum
- Unique US students with 5+ touchpoints of CSA/CSP or post-AP units for HS
- Total US students = Total unique known students (including 1-5 day HS) + 40% uplift for ES
*/

with participating as (
    select * from 
    {{ref('dim_participating_students')}}
    where qualifying_date >= '2019-07-01' --starting with 2019-20 school year
)

, dssla as (
    select 
        student_id,
        school_year
    from 
        {{ref('dim_student_script_level_activity')}}
    where 
        user_type = 'student' and
        country = 'united states' and 
        course_name in
            ('csf','csc k-5',
            'csd','6-8 special topics','csc 6-8',
            'csa','csp','9-12 special topics','foundations of cs') and
        activity_date >= '2019-07-01' --starting with 2019-20 school year
)

, category_counts as (
    select 
    school_year
    , count (distinct case when grade_band = 'HS' then participating.student_id else null end ) n_students_HS
    , count (distinct case when grade_band = 'MS' then participating.student_id else null end ) n_students_MS
    , count (distinct case when grade_band = 'ES' then participating.student_id else null end ) n_students_ES 
    , count (distinct case when grade_band = 'HS' and gender_group = 'f' then participating.student_id else null end ) n_students_HS_f
    , count (distinct case when grade_band = 'HS' and gender_group in ('m','nb') then participating.student_id else null end ) n_students_HS_not_f
    , count (distinct case when grade_band = 'HS' and race_group in ('hispanic','black','two_or_more_urg','american_indian','hawaiian') then participating.student_id else null end ) n_students_HS_urg
    , count (distinct case when grade_band = 'HS' and race_group in ('white','asian','two_or_more_non_urg') then participating.student_id else null end ) n_students_HS_not_urg
    , count (distinct case when grade_band = 'MS' and gender_group = 'f' then participating.student_id else null end ) n_students_MS_f
    , count (distinct case when grade_band = 'MS' and gender_group in ('m','nb') then participating.student_id else null end ) n_students_MS_not_f
    , count (distinct case when grade_band = 'MS' and race_group in ('hispanic','black','two_or_more_urg','american_indian','hawaiian') then participating.student_id else null end ) n_students_MS_urg
    , count (distinct case when grade_band = 'MS' and race_group in ('white','asian','two_or_more_non_urg') then participating.student_id else null end ) n_students_MS_not_urg
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
    --, total_counts.n_students as n_students
    , total_counts.n_students + n_students_ES * 0.4 as n_students_adj
    , n_students_HS
    , cast(n_students_HS_f as decimal) / (n_students_HS_not_f + n_students_HS_f) * n_students_HS as n_students_HS_f_calc
    , cast(n_students_HS_urg as decimal) / (n_students_HS_not_urg + n_students_HS_urg) * n_students_HS as n_students_HS_urg_calc
    , n_students_MS
    , cast(n_students_MS_f as decimal) / (n_students_MS_not_f + n_students_MS_f) * n_students_MS as n_students_MS_f_calc
    , cast(n_students_MS_urg as decimal) / (n_students_MS_not_urg + n_students_MS_urg) * n_students_MS as n_students_MS_urg_calc
    --, n_students_ES
    , n_students_ES * 1.4 as n_students_ES_adj
    from category_counts
    join total_counts
        on category_counts.school_year = total_counts.school_year
    order by school_year desc
)

select * 
from final