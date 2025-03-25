/* Author: Cory
Date: 11/30/24
Purpose: Used for establishing 2030 participating student goals
Description:
This dim file creates a row for each student, grade band, school year and then the qualifying date (1 day for ES, 5 days for HS)
NOTE: int_participating_students can have multiple rows for a given student, school_year, and grade band. This file cannot

Description of qualifying criteria
- ES: Unique US students with 1+ touchpoint of ES curriculum + 40% uplift based on assumption of anonymous/signed-out usage
- MS: Unique US students with 1+ touchpoint of MS curriculum
- HS: Unique US students with 5+ touchpoints of CSA/CSP or standalone units for HS
- Total US students = Total unique known students (including 1-5 day HS) + ES *1.4


Edit log: 
3/25 - edited to include global usage
*/

with

int_curriculum_students as (
    select * from 
    {{ref('int_curriculum_students')}}
)

, students as (
    select 
        student_id,
        race_group,
        gender_group
    from 
    {{ref('dim_students')}}
)

, earliest_date as (
    select 
        int_curriculum_students.school_year as school_year,
        int_curriculum_students.student_id as student_id,
        int_curriculum_students.grade_band as grade_band,
        country,
        case when country = 'united states' then 'us'
             when country <> 'united states' then 'intl'
             else null 
        end as us_intl,
        min(qualifying_date) as qualifying_date
    from 
        int_curriculum_students
    left join students 
        on students.student_id = int_curriculum_students.student_id
    group by 1,2,3,4
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

