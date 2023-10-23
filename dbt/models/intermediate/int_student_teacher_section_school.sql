-- rebuild: student_teacher_section_complete

with 
school_years as (
    select *
    from {{ ref('int_school_years') }}
),

course_structure as (
    select * 
    from {{ ref('dim_course_structure') }}
),

user_levels as (
    select 
        user_id as student_id,
        script_id,
        level_id,
        user_level_created_at
    from {{ ref('dim_user_levels') }}
    where attempts > 0
),

combined as (
    select 
        -- user level data 
        ul.student_id,
        ul.created_at as first_user_level_created_at,
        
        -- school year
        sy.school_year,
        
        -- course data
        cs.course_name_true as course_name,
        cs.script_id,
        cs.stage_id,
        cs.level_id,

        -- get the first ul.created_at for this student+school_year
        row_number() over(partition by 
            ul.student_id, 
            sy.school_year 
            order by ul.created_at asc) as row_num  -- (js) revisit later
    from user_levels as ul
    left join course_structure as cs 
        on ul.script_id = cs.script_id
        and ul.level_id = cs.level_id
    join school_years as sy 
        on ul.created_at 
            between sy.started_at and sy.ended_at
)

select * 
from final 
where row_num = 1