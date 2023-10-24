/* 
1. Design:
    section_id int,
    school_year varchar(10),
    is_active bit

2. Definitions:
    is_active:  5+ students completing 
                at least 1 level
    " a section is active = 1 in a given SY if it 
        1) has 5+ students in it and 
        2) 5+ students are completing 1+ levels (of any course) (edited) "

3. Sources:
    dim_user_levels
    int_student_teacher_section_complete

Ref: dataops-316
*/

with
student_teacher_section_school as (
    select 
        section_id,
        school_year,
        count(distinct student_id) as total_students
    from {{ ref('int_student_teacher_section_school') }}
    {{ dbt_utils.group_by(2) }}
),

user_levels as (
    select 
        user_id
    from {{ ref('dim_user_levels') }}
    where user_id in (select student_id from student_teacher_section_school)
),

combined as (
    select 
        section_id,
        school_year, 
        total_students,
        count(distinct case when user_levels.user_id is not null then student_id end) as is_completing
    from student_teacher_section_school as stss 
    left join user_levels 
        on stss.student_id = user_levels.user_id
    {{ dbt_utils.group_by(4) }}
),

final as (
    select 
        section_id, 
        school_year,
        case when total_students > 5 
             and is_completing > 5 
        then 1 else 0 end as is_active
    from combined 
)

select *
from final