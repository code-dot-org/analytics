



with 
user_levels as (
    select * 
    from "dev"."dbt_allison"."stg_dashboard__user_levels"
),

levels as (
    select * 
    from "dev"."dbt_allison"."dim_levels"
),

course_structure as (
    select * 
    from "dev"."dbt_allison"."dim_course_structure"
),

school_years as (
    select * 
    from "dev"."dbt_allison"."int_school_years"
),

students as (
    select * 
    from "dev"."dbt_allison"."dim_students"
),


user_geos as (
    select * 
    from "dev"."dbt_allison"."stg_dashboard__user_geos"
    where is_international = 0
),

combined as (
    select 
        ul.user_level_id,
        ul.user_id,
        ul.created_at as user_level_created_at,
        ul.level_id,
        ul.level_source_id,
        cs.stage_id,
        ul.script_id,
        sy.school_year,
        cs.course_name_true                     as course_name
        
        
    from user_levels as ul 
     join course_structure as cs 
        on ul.script_id = cs.script_id
    join school_years as sy 
        on ul.created_at between sy.started_at and sy.ended_at
    join students as stu 
        on ul.user_id = stu.student_user_id
    join levels as lev 
        on ul.level_id = lev.level_id
    join user_geos as ug 
        on stu.student_user_id = ug.user_id
    group by 1,2,3,4,5,6,7,8,9
)

select * 
from combined