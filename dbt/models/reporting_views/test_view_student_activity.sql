{#
model: tableau_view_course_utilization
auth: natalia
notes:
changelog:
#}

with 
course_structure as (
    select *
    from {{ ref('dim_course_structure') }} 
),

student_script_level_activity as (
    select *
    from {{ ref('dim_student_script_level_activity') }} 
),

current_year_student_courses as (
    select * from course_structure cs
    where cs.version_year = '2024'
    and  cs.participant_audience = 'student'
),

current_year_activity as (
    select sla.*
    from student_script_level_activity sla
    join current_year_student_courses cyc on sla.script_id = cyc.script_id and sla.level_id = cyc.level_id
)

select * 
from current_year_activity


