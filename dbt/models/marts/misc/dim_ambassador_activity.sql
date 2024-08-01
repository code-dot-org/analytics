with 

foorm_survey as (
    select * 
    from {{ ref('dim_foorms') }}
    where form_name = 'surveys/teachers/young_women_in_cs'
),

sections as (
    select * 
    from {{ ref('stg_dashboard__sections') }}
),

followers as (
    select * 
    from {{ ref('stg_dashboard__followers') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

user_levels as (
    select * 
    from {{ ref('stg_dashboard__user_levels') }}
),

course_structure as (
    select * 
    from {{ ref('dim_course_structure') }}
)

select distinct 
    fs.user_id
    , fs.code_studio_name
    , fs.email
    , s.section_id
    , s.section_name
    , s.created_at                                                  as section_created_dt
    , sy.school_year                 
    , f.student_id 
    , cs.course_name_true                                           as course_name
    , cs.script_name 
from foorm_survey                                                   as fs

left join sections                                                  as s 
    on fs.user_id = s.teacher_id

left join followers                                                 as f 
    on s.section_id = f.section_id 

left join school_years                                              as sy 
    on f.student_added_at between sy.started_at and sy.ended_at -- school year when the followers were added to the section

left join user_levels                                               as ul 
    on f.student_id = ul.user_id 
	and trunc(f.student_added_at) <= trunc(ul.created_at)  
	and ul.created_at between sy.started_at and sy.ended_at -- user activity for section participants (followers) after they were added to the section and in the same school year when the followers were added to the section

left join course_structure                                          as cs 
    on ul.script_id = cs.script_id  
    and ul.level_id = cs.level_id 

where fs.item_name = 'teacher_account'
and fs.response_value = 'true'