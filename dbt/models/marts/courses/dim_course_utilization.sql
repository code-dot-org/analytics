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
    select *
    from {{ ref('dim_user_levels') }}
),

school_status as (
    select 
        school_id,
        school_year,
        status as school_status
    from {{ ref('dim_school_status') }}
),
  
schools as (
    select *
    from {{ ref('dim_schools') }}
    where school_id in (
        select school_id 
        from school_status )
),

teachers as (
    select 
        teacher_id,
        school_year,
        status as teacher_status 
    from {{ ref('dim_teacher_status') }}
),

active_sections as (
    select *,
        1 as is_active
    from {{ ref('int_active_sections') }}
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
        
        -- {#
        cs.is_student,
        cs.is_pd,
        cs.is_self_paced,
        -- #}

        -- sections 
        ias.section_id,
        ias.teacher_id,
        ias.is_active   as is_active_section,

        -- schools 
        ias.school_year,
        ias.school_id,
        sst.school_status, 
        sc.school_district_id,
        sc.school_district_name,
        
        -- teachers 
        tea.teacher_status,

        -- dates
        ul.created_dt          as activity_date,


        -- aggregations 
        sum(ul.attempts)       as num_attempts,
        max(ul.best_result)    as best_result,
        sum(ul.time_spent)     as time_spent

    from course_structure                       as cs 

    left join user_levels                       as ul 
        on  cs.script_id = ul.script_id
        and cs.level_id  = ul.level_id 
    
    left join teachers                          as tea
         on tea.teacher_id  = ias.teacher_id 
        and tea.school_year = ias.school_year
    
    left join school_status                     as sst 
         on sst.school_id   = ias.school_id
        and sst.school_year = ias.school_year

    left join schools                           as sc 
        on sst.school_id = sc.school_id 
    
    left join active_sections               as ias 
         on tea.teacher_id  = ias.teacher_id 
        and tea.school_year = ias.school_year 

    {{ dbt_utils.group_by(19) }} )

select course_name, section_id, teacher_id
from combined
limit 5