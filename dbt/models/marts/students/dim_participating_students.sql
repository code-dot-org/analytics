/* Author: Cory
Date: 11/30/24
Purpose: Used for establishing 2030 participating student goals
Description:
This dim file creates a row for each student, grade band, course_or_module and then the qualifying date (1 day for ES, 5 days for HS)
NOTE: int_participating_students can have multiple rows for a given student, school_year, and grade band. This file cannot - we show only their first qualifying date 

Edit log: 

Description
- Unique US students with 1+ touchpoint of ES curriculum + 40% uplift
- Unique US students with 1+ touchpoint of MS curriculum
- Unique US students with 5+ touchpoints of CSA/CSP or standalone units for HS
- Total US students = Total unique known students (including 1-5 day HS) + 40% uplift for ES
*/

with

int_participating_students as (
    select * from 
    {{ref('int_participating_students')}}
),

students as (
    select 
        student_id,
        race_group,
        gender_group
    from 
    {{ref('dim_students')}}
),

earliest_date as (
    select 
        int_participating_students.school_year as school_year,
        int_participating_students.student_id as student_id,
        int_participating_students.grade_band as grade_band,
        min(qualifying_date) as qualifying_date
    from 
        int_participating_students
    left join students 
        on students.student_id = int_participating_students.student_id
    group by 1,2,3
)

, final as (
    select
        earliest_date.*,
        students.race_group,
        students.gender_group
    from earliest_date
    left join students 
        on students.student_id = earliest_date.student_id
)

select * 
from final

