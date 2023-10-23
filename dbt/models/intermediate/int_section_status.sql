/* 
1. Design:
    section_id int,
    school_year varchar(10),
    is_active bit

2. Definitions:
    is_active:  5+ students completing 
                at least 1 level
    " a section is active = 1 in a given SY if it 1) has 5+ students in it and 2) 5+ students are completing 1+ levels (of any course) (edited) "

3. Sources:
    dim_user_levels
    int_student_teacher_section_complete

Ref: dataops-316
*/

student_teacher_section_school as (
    select *
    from {{ ref('int_student_teacher_section_school') }}
),

user_levels as (
    select * 
    from {{ ref('dim_user_levels') }}
    where user_type = 'student'
),

combined as (
    select section_id,
        course_id,
        school_year, 
        teacher_id,
        count(distinct student_id) as total_students
    from student_teacher_section_school as stss 
    left join user_levels 
        on stss.student_id = user_levels.user_id
    group by 1,2,3,4
),

final as (
    select section_id, 
        school_year,
        case when total_students > 5 then 1 else 0 end as is_active
    from combined 
)

select *
from final