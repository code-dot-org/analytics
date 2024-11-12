/* Author: Cory
Date: 11/12/24
Purpose: Used for establishing 2030 participating student goals for HS/MS/ES segments

Description
- Unique US students with 1+ day of any curriculum (not HOC)

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

select
    school_year,
    count(distinct student_id)
from dssla
group by school_year
order by school_year desc