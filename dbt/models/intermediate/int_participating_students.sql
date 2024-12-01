/* Author: Cory
Date: 11/30/24
Purpose: Used for establishing 2030 participating student goals
Description
- Unique US students with 1+ touchpoint of ES curriculum + 40% uplift
- Unique US students with 1+ touchpoint of MS curriculum
- Unique US students with 5+ touchpoints of CSA/CSP or post-AP units for HS
- Total US students = Total unique known students (including 1-5 day HS) + 40% uplift for ES
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

-- NZM provided this logic
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

, days_per_student_course as ( --groups and orders by date
    select 
    dssla.school_year, 
    dssla.student_id,
    dssla.school_state
    , case 
        when dssla.course_name in ('csa', 'csp', 'foundations of cs','9-12 special topics') then 'HS'
        when dssla.course_name in ('csd', 'ai') then 'MS' -- Needs to be adjusted once changes to Course structure are live 
        else 'ES'
        end grade_band
    , coalesce(sm.unit, dssla.course_name) course_or_module
    , activity_date
    from dssla 
    left join standalone_modules sm 
        on dssla.course_name = sm.course_name and dssla.unit_name = sm.unit
    group by 1,2,3,4,5,6
)

select *,
    row_number() over (partition by student_id, school_year, course_or_module order by activity_date asc) as day_order
from days_per_student_course