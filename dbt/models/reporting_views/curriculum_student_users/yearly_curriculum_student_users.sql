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

with curriculum_students as (
    select *
    from 
        {{ref('dim_curriculum_student_users')}}
    where qualifying_date >= '2020-07-01' --starting with 2020-21 school year
)

, dssla as (
    select 
        student_id
        , school_year
    from 
        {{ref('dim_student_script_level_activity')}}
    where 
        user_type = 'student' and
        content_area <> 'hoc' and
        activity_date >= '2019-07-01' --starting with 2019-20 school year
)

, category_counts as (
    select 
    school_year
    , us_intl
    , count (distinct case when grade_band = 'HS' then curriculum_students.student_id else null end ) n_students_HS
    , count (distinct case when grade_band = 'MS' then curriculum_students.student_id else null end ) n_students_MS
    , count (distinct case when grade_band = 'ES' then curriculum_students.student_id else null end ) n_students_ES 
    , count (distinct case when grade_band = 'HS' and gender_group = 'f' then curriculum_students.student_id else null end ) n_students_HS_f
    , count (distinct case when grade_band = 'HS' and gender_group in ('m','nb') then curriculum_students.student_id else null end ) n_students_HS_not_f
    , count (distinct case when grade_band = 'HS' and race_group in ('hispanic','black','two_or_more_urg','american_indian','hawaiian') then curriculum_students.student_id else null end ) n_students_HS_urg
    , count (distinct case when grade_band = 'HS' and race_group in ('white','asian','two_or_more_non_urg') then curriculum_students.student_id else null end ) n_students_HS_not_urg
    , count (distinct case when grade_band = 'MS' and gender_group = 'f' then curriculum_students.student_id else null end ) n_students_MS_f
    , count (distinct case when grade_band = 'MS' and gender_group in ('m','nb') then curriculum_students.student_id else null end ) n_students_MS_not_f
    , count (distinct case when grade_band = 'MS' and race_group in ('hispanic','black','two_or_more_urg','american_indian','hawaiian') then curriculum_students.student_id else null end ) n_students_MS_urg
    , count (distinct case when grade_band = 'MS' and race_group in ('white','asian','two_or_more_non_urg') then curriculum_students.student_id else null end ) n_students_MS_not_urg
    from participating
    group by school_year, us_intl
)

, total_counts as (
    select
    school_year
    , us_intl
    , count (distinct student_id) n_students
    from
        dssla
    group by
        school_year, us_intl
)

, final as (select
    category_counts.school_year
    , us_intl
    --, total_counts.n_students as n_students
    , round(total_counts.n_students + n_students_ES * 0.4)::int as n_students_adj
    , n_students_HS
    , round(cast(n_students_HS_f as decimal) / (n_students_HS_not_f + n_students_HS_f) * n_students_HS)::int as n_students_HS_f_calc
    , round(cast(n_students_HS_urg as decimal) / (n_students_HS_not_urg + n_students_HS_urg) * n_students_HS)::int as n_students_HS_urg_calc
    , n_students_MS
    , round(cast(n_students_MS_f as decimal) / (n_students_MS_not_f + n_students_MS_f) * n_students_MS)::int as n_students_MS_f_calc
    , round(cast(n_students_MS_urg as decimal) / (n_students_MS_not_urg + n_students_MS_urg) * n_students_MS)::int as n_students_MS_urg_calc
    --, n_students_ES
    , round(n_students_ES * 1.4)::int as n_students_ES_adj
    from category_counts
    join total_counts
        on category_counts.school_year = total_counts.school_year
    group by us_intl
    order by school_year desc
)

select * 
from final