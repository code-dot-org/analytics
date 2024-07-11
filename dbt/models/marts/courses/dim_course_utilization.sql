-- model: dim_course_utilization
-- scope: capture all course usage for teachers and students
-- author: jspringer

with 
course_structure as (
    select *
    from {{ ref('dim_course_structure') }}
),

user_levels as (
    select *
    from {{ ref('dim_user_levels') }}
    where user_id = 55870217 --testing 

),

schools as (
    select 
        school_id,
        school_year,
        status as school_status
    from {{ ref('dim_school_status')}}
),

teachers as (
    select 
        teacher_id,
        school_year,
        status as teacher_status 
    from {{ ref('dim_teacher_status') }}
),

active_sections as (
    select *, 1 as is_active
    from {{ ref('int_active_sections')}}
),

sections as (
    select *
    from {{ ref('dim_sections') }}
),

combined as (
    select 
        -- course structure 
        cs.course_name,
        cs.script_name,
        cs.level_name,
        cs.stage_name,
        cs.unit as unit_name,

        -- sections 
        sec.section_id,
        sec.teacher_id,
        sec.student_id,
        sec.school_id,
        sec.school_year,
        
        -- teachers 
        tst.teacher_status,
        count(distinct tst.teacher_id) as num_teachers,

        -- schools
        sst.school_status, 
        count(distinct sst.school_id) as num_schools,
        
        -- user levels 
        sum(attempts) as num_attempts,
        max(best_result) as best_result

    from course_structure   as cs 
    left join user_levels   as ul 
        on ul.script_id = cs.script_id
        and ul.level_id = cs.level_id 

    left join section_mapping as sec 
        on ???
    
    left join teacher_status as tst
        on tst.teacher_id = sec.teacher_id 
        and tst.school_year = sec.school_year
    
    left join school_status as sst 
        on sst.school_id = sec.school_id
        and sst.school_year = sec.school_year

    {{ dbt_utils.group_by(11) }} )

select * 
from combined