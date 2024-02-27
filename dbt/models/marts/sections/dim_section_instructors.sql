-- model: dim_section_instructors
-- scope: invited co-teachers

-- option 1: dim_section_instructors
-- this is BP in terms of bringing in this data. we would then affect dim_teachers using this dim model.


with 
section_instructors as (
    select * 
    from {{ ref('stg_dashboard__section_instructors') }}
),

courses as (
    select * 
    from {{ ref('dim_course_structure') }}
),

combined as (
    select 
        sei.instructor_id   as user_id, -- teacher_id 
        sei.invited_by_id   as invited_by_user_id, -- teacher who invited this teacher
        sei.section_id,
        sei.status          as section_instructor_status, -- status of this teacher in this section  

        -- course info 
        courses.course_id,
        courses.course_name,
        courses.course_name_long,        
        courses.script_id,
        courses.level_id,

        -- metadata
        sei.created_at,
        sei.updated_at
    from section_instructors as sei 
    left join courses
        on section_instructors.section_id = courses.section_id)

select *
from combined

