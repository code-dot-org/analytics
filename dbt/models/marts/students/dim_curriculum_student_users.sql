/* Author: Cory
Date: 11/30/24
Purpose: Used for establishing 2030 participating student goals
Description:
This dim file creates a row for each student, grade band, school year and then the qualifying date (1 day for ES, 5 days for HS)
NOTE: int_participating_students can have multiple rows for a given student, school_year, and grade band if they qualify in multiple courses. This model does not, it takes the earliest qualifying date per student in each grade band. 


Edit log: 
3/25/25 - CK edited to include global usage rather than just US +  consolidated the logic and moved the race/gender fields into the int_curriculum_students table
*/

with

final as (
    select 
        school_year
        , student_id
        , grade_band
        , country
        , us_intl
        , race_group
        , gender_group
        , min(qualifying_date) as qualifying_date
    from 
        {{ref('int_curriculum_students')}}
    group by 1,2,3,4,5,6,7

)

select * 
from final 

