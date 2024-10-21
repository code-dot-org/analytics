/* 

    model: not a full model, a tool for identifying unmatched heavy user schools in the AP ledgers
    auth: cory
    notes:Uses the student data from ballmer_heavy_users and filters for schools with at least 5 students in the completed set for that school year and course
    changelog:

Full context:
CB gives us a full list of schools that take the exam as well as those that use our curriculum. We also use our list of heavy user schools, and CB gives us the crosswalk to find the AI codes for those schools. But that crosswalk is only for public schools.
This view helps identify heavy user private schools that do not have a corresponding AI Code in the crosswalk, and thus will be missed unless we match them up manually
Long term recommendation to do this via fuzzy matching. For 2024, I did matching of school names 

*/

with bhus as (
    select * from {{ref('ballmer_heavy_user_schools')}}
    where school_year = '2023-24' 
    and heavy_user_school_flag = 1
),

schools as (
    select * from {{ref('dim_schools')}}
),

ledgers as (
    select * from {{ref('dim_ap_ledgers')}}
    where school_year = '2023-24' --only this year
),

final as (
    select 
    bhus.school_id, 
    bhus.course_name,
    schools.school_name,
    schools.city, 
    schools.state, 
    schools.zip,
    last_survey_year, 
    is_stage_hi, 
    school_category, 
    school_type,
    ledgers.ai_code --will be missing for the ones that aren't matched
    FROM bhus 
    left join ledgers on bhus.school_id = ledgers.school_id 
    left join schools on bhus.school_id = schools.school_id -- bringing over supplementary info
)

select * 
from final
where ai_code is Null
and school_type = 'private'
order by state desc

