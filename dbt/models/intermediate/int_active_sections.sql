{# Ref: dataops-316 #}

with
student_section as (
    select *
    from {{ ref('int_section_mapping') }}
),

section_instructors as (
    select 
         teacher_id
        ,section_id
    from {{ ref('stg_dashboard__section_instructors') }}
),

student_course_starts as (
    select *
    from {{ ref('dim_user_course_activity') }}
    where user_id in (select student_id from student_section)
),

combined as (
    select 
         ss.teacher_id
		,ss.school_year
		,scs.course_name
		,ss.section_id
        ,case 
            when si.teacher_id = ss.teacher_id 
                then 1 else 0 
            end as is_section_owner

        ,min(scs.first_activity_at)     as section_started_at
		,count(distinct ss.student_id)  as num_students 

	from student_course_starts  as scs
	join student_section        as ss 
        on scs.user_id = ss.student_id 
        and scs.school_year = ss.school_year 
    join section_instructors    as si
        on ss.section_id = si.section_id
	{{ dbt_utils.group_by(5) }}
),

final as (
    select 
         teacher_id
        ,school_year
        ,course_name
        ,section_id
        ,is_section_owner
        ,section_started_at
        ,num_students
    from combined
    where num_students >= 5
)

select *
from final
