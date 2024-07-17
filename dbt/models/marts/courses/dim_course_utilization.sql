-- model: dim_course_utilization
-- scope: capture all course usage for teachers and students
-- grain: script, level, and day/week
-- author: jspringer

with 
course_structure as (
    select *
    from {{ ref('dim_course_structure') }}
),

user_levels as (
    select *,

        date_trunc(
            'date',
            created_at) as created_dt

    from {{ ref('dim_user_levels') }}
),

school_status as (
    select 
        school_id,
        school_year,
        status as school_status
    from {{ ref('dim_school_status')}}
),
  
schools as (
    select *
    from {{ ref('dim_schools') }}
    where school_id in (
        select school_id from school_status )
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
        cs.unit         as unit_name,
        
        {#
            cs.is_student,
            cs.is_pd,
            cs.is_self_paced,
        #}

        -- sections 
        ias.section_id,
        ias.teacher_id,
        ias.student_id,
        ias.is_active_section,

        -- schools 
        sec.school_year,
        sec.school_id,
        sst.school_status, 
        sc.school_district_id
        sc.school_district_name,
        
        -- teachers 
        tst.teacher_status,

        -- aggregations 
        sum(ul.attempts)       as num_attempts,
        max(ul.best_result)    as best_result,
        sum(ul.time_spent)     as time_spent,

        ul.created_dt          as activity_date

    from course_structure   as cs 
    left join user_levels   as ul 
        on  ul.script_id = cs.script_id
        and ul.level_id = cs.level_id 
    
    left join teacher_status as tst
        on  tst.teacher_id = sec.teacher_id 
        and tst.school_year = sec.school_year
    
    left join school_status as sst 
        on  sst.school_id = sec.school_id
        and sst.school_year = sec.school_year

    join schools as sc 
        on sst.school_id = sc.school_id 
        -- and sst.school_year = sd.last_known_school_year_open ?
    
    left join int_active_sections as ias 
        on  tst.teacher_id = ias.teacher_id 
        and tst.school_year = ias.school_year 

    {{ dbt_utils.group_by(11) }} )

select * 
from combined