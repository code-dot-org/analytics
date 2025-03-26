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
3/25/25 - CK edited to include global usage rather than just US
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

