{# dataops-316 #}

with
student_section as (
    select *
    from {{ ref('int_section_mapping') }}
)

, student_course_starts as (
    select *
    from {{ ref('dim_user_course_activity') }}
    where user_id in (select student_id from student_section)
    --and user_type = 'student'
)

, ranks as (
    select
        ss.student_id
        , ss.teacher_id
        , ss.school_year
        , scs.course_name
        , ss.section_id
        , first_activity_at
        , row_number() over (
            partition by ss.section_id, ss.teacher_id, ss.school_year, scs.course_name
            order by first_activity_at
        ) as student_row_n
    from student_course_starts scs
	join student_section ss 
	on scs.user_id = ss.student_id 
    and scs.school_year = ss.school_year 
)

, fifth_rank as (
    select *
    from ranks
    where student_row_n = 5
)

, combined as (
    select 
        ranks.teacher_id
		, ranks.school_year
		, ranks.course_name
		, ranks.section_id
        , fifth_rank.first_activity_at as fifth_student_started_at
        , min(ranks.first_activity_at) as section_started_at
		, count(distinct ranks.student_id) as num_students 
	from ranks 
    left join fifth_rank
        on ranks.teacher_id = fifth_rank.teacher_id
        and ranks.school_year = fifth_rank.school_year
        and ranks.course_name = fifth_rank.course_name
        and ranks.section_id = fifth_rank.section_id
	group by 1,2,3,4,5
)

, final as (
    select 
         teacher_id
        , school_year
        , course_name
        , section_id
        , section_started_at
        , fifth_student_started_at
        , num_students
    from combined
    where num_students >= 5
)

select *
from final