/* Author: Cory
Date: 11/12/24
Purpose: Used for establishing 2030 participating student goals (overall)

Description
- Unique US students with 1+ day of any curriculum (not HOC)
- Assumes 30% uplift for anonymous

Future work:
- Change to target area rather than curriculum mapping when course_structure is available
- Replace 30% inflation factor with actual anonymous data once it's collected via statsig

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
    count(distinct student_id) * 1.3
from dssla
group by school_year
order by school_year desc