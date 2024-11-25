/* Author: Cory
Date: 11/12/24
Purpose: Fact table with calculations

Description
- Unique US students with 1+ touchpoint of ES curriculum + 40% uplift
- Unique US students with 1+ touchpoint of MS curriculum
- Unique US students with 5+ touchpoints of CSA/CSP or post-AP units for HS
- Total US students = Total unique known students (including 1-5 day HS) + 40% uplift for ES

Future work:
- Replace 40% uplift with anonymous student data when available from statsig

Edit log: 
*/

with

dps as (
    select * from 
    {{ref('dim_participating_student')}} 
),

days_per_student_course as (
    select school_year,
        student_id,
        grade_band,
        course_or_module,
        count(distinct activity_date) n_days_of_activity,
        min(activity_date) first_activity_date,
        case
            when day_order = 1
            then activity_date
            else null
            end day1_date
    from dps
    group by 1, 2, 3, 4
)

select * from days_per_student_course

, calculated as (
    select 
    school_year
    , count (distinct case when grade_band = 'HS' and n_days_of_activity >= 5 then days_per_student_course.student_id else null end ) n_students_HS
    , count (distinct case when grade_band = 'MS' then days_per_student_course.student_id else null end ) n_students_MS
    , count (distinct case when grade_band = 'ES' then days_per_student_course.student_id else null end ) n_students_ES 
    , count (distinct case when grade_band = 'HS' and gender_group = 'f' and n_days_of_activity >= 5 then days_per_student_course.student_id else null end ) n_students_HS_f
    , count (distinct case when grade_band = 'HS' and gender_group in ('m','nb') and n_days_of_activity >= 5 then days_per_student_course.student_id else null end ) n_students_HS_not_f
    , count (distinct case when grade_band = 'HS' and race_group in ('hispanic','black','two_or_more_urg','american_indian','hawaiian') and n_days_of_activity >= 5 then days_per_student_course.student_id else null end ) n_students_HS_urg
    , count (distinct case when grade_band = 'HS' and race_group in ('white','asian','two_or_more_non_urg') and n_days_of_activity >= 5 then days_per_student_course.student_id else null end ) n_students_HS_not_urg
    , count (distinct case when grade_band = 'MS' and gender_group = 'f' then days_per_student_course.student_id else null end ) n_students_MS_f
    , count (distinct case when grade_band = 'MS' and gender_group in ('m','nb') then days_per_student_course.student_id else null end ) n_students_MS_not_f
    , count (distinct case when grade_band = 'MS' and race_group in ('hispanic','black','two_or_more_urg','american_indian','hawaiian') then days_per_student_course.student_id else null end ) n_students_MS_urg
    , count (distinct case when grade_band = 'MS' and race_group in ('white','asian','two_or_more_non_urg') then days_per_student_course.student_id else null end ) n_students_MS_not_urg
    from days_per_student_course
    left join students
        on students.student_id = days_per_student_course.student_id
    group by school_year
)

select
    school_year
    , n_students_HS
    , cast(n_students_HS_f as decimal) / (n_students_HS_not_f + n_students_HS_f) * n_students_HS as n_students_HS_f_calculated
    , cast(n_students_HS_urg as decimal) / (n_students_HS_not_urg + n_students_HS_urg) * n_students_HS as n_students_HS_urg_calculated
    , n_students_MS
    , cast(n_students_MS_f as decimal) / (n_students_MS_not_f + n_students_MS_f) * n_students_MS as n_students_MS_f_calculated
    , cast(n_students_MS_urg as decimal) / (n_students_MS_not_urg + n_students_MS_urg) * n_students_MS as n_students_MS_urg_calculated
    , n_students_ES * 1.4 as n_students_ES_adjusted
from calculated
where n_students_hs > 0
order by school_year desc