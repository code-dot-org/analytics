/* Author: Cory
Date: 11/12/24
Purpose: Used for establishing 2030 participating student goals for India/Mexico segment

Description
- Unique India or Mexico students with 1+ day of any curriculum (CSF or CSD)

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
        country in ('india', 'mexico') and 
        course_name in ('csf','csd')

)

select
    school_year,
    count(distinct student_id)
from dssla
group by school_year
order by school_year desc