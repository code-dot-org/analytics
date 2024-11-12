/* Author: Cory
Date: 11/12/24
Purpose: Used for establishing 2030 participating student goals for HS/MS/ES segments

Description
- Unique US students with 1+ touchpoint of ES curriculum
- Unique US students with 1+ touchpoint of MS curriculum
- Unique US students with 5+ touchpoints of CSA/CSP or post-AP units for HS

Future work:
- Change to target area rather than curriculum mapping when course_structure is available

Edit log: 
*/


with

dssla as (
    select * 
    from {{ref('dim_student_script_level_activity')}}
    where 
        user_type = 'student' and
        country = 'united states' and 
        course_name in ('csa','csp','foundations of cs',
            'csf','csc','csd','ai','9-12 special topics')

)
, students as (
    select * from 
    {{ref('dim_students')}}
)

, standalone_modules as (
    select distinct course_name, unit
    from {{ref('dim_course_structure')}} cs 
    where 
    cs.unit in (
            'csa-consumer-review-lab'
            , 'csa-data-lab'
            , 'csa-labs'
            , 'csa-magpie-lab'
            , 'csa-postap-se-and-computer-vision'
            , 'csa-software-engineering'
        )
    or (
        cs.course_name in ('csd', 'csa')
        and cs.is_active_student_course = 1
        and is_standalone = 'true'
        and cs.unit not like 'tess-test-csa'
        )
    )

, days_per_student_course as (
    select 
    dssla.school_year, 
    dssla.student_id 
    , case 
        when dssla.course_name in ('csa', 'csp', 'foundations of cs','9-12 special topics') then 'HS'
        when dssla.course_name in ('csd', 'ai') then 'MS' -- Needs to be adjusted once changes to Course structure are live 
        else 'ES'
        end grade_band
    , coalesce(sm.unit, dssla.course_name) course_or_module
    , count(distinct dssla.activity_date) n_days_of_activity 
    from dssla 
    left join standalone_modules sm 
        on dssla.course_name = sm.course_name and dssla.unit_name = sm.unit
    group by 1,2,3,4
)

select
    school_year
    , count (distinct case when grade_band = 'HS' then days_per_student_course.student_id else null end ) n_students_HS
    , count (distinct case when grade_band = 'MS' then days_per_student_course.student_id else null end ) n_students_MS
    , count (distinct case when grade_band = 'ES' then days_per_student_course.student_id else null end ) n_students_ES
    , count (distinct case when grade_band = 'HS' and gender_group = 'f' then days_per_student_course.student_id else null end ) n_students_HS_f
    , count (distinct case when grade_band = 'HS' and gender_group in ('m','nb') then days_per_student_course.student_id else null end ) n_students_HS_not_f
    , count (distinct case when grade_band = 'HS' and race_group in ('hispanic','black','two_or_more_urg','american_indian','hawaiian') then days_per_student_course.student_id else null end ) n_students_HS_urg
    , count (distinct case when grade_band = 'HS' and race_group in ('white','asian','two_or_more_non_urg') then days_per_student_course.student_id else null end ) n_students_HS_not_urg
    , count (distinct case when grade_band = 'MS' and gender_group = 'f' then days_per_student_course.student_id else null end ) n_students_MS_f
    , count (distinct case when grade_band = 'MS' and gender_group in ('m','nb') then days_per_student_course.student_id else null end ) n_students_MS_not_f
    , count (distinct case when grade_band = 'MS' and race_group in ('hispanic','black','two_or_more_urg','american_indian','hawaiian') then days_per_student_course.student_id else null end ) n_students_MS_urg
    , count (distinct case when grade_band = 'MS' and race_group in ('white','asian','two_or_more_non_urg') then days_per_student_course.student_id else null end ) n_students_MS_not_urg
    from days_per_student_course
    left join students
        on students.student_id = days_per_student_course.student_id
    where 
        grade_band = 'ES' or grade_band = 'MS' or n_days_of_activity >= 5
    group by school_year
    order by school_year desc