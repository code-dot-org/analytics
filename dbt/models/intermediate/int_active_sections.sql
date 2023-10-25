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
student_section as (
    select *
    from {{ ref('int_student_section') }}
),

student_course_starts as (
    select *
    from {{ ref('fct_student_course_starts') }}
    where student_id in (select student_id from student_section)
),

combined as (
    select ss.teacher_id
		,ss.school_year
		,scs.course_name
		,ss.section_id
		,count(distinct ss.student_id) as num_students 
	from student_course_starts scs
	join student_section ss 
	on scs.student_id = ss.student_id 
        and scs.school_year = ss.school_year 
	group by 1,2,3,4
),

final as (
    select teacher_id
        , school_year
        , course_name
        , section_id
        , 1 as active
    from combined
    where num_students >= 5
)

select *
from final