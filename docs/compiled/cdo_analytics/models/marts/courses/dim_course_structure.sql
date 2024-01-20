

with 
scripts as (
    select *
    from "dev"."dbt_jordan"."stg_dashboard__scripts" 
),

levels as (
    select * 
    from "dev"."dbt_jordan"."stg_dashboard__levels"
    ),

stages as (
    select *
    from "dev"."dbt_jordan"."stg_dashboard__stages"
),

script_levels as (
    select *
    from "dev"."dbt_jordan"."stg_dashboard__script_levels"
),

levels_script_levels as (
    select * 
    from "dev"."dbt_jordan"."stg_dashboard__levels_script_levels"
),

course_scripts as (
    select * 
    from "dev"."dbt_jordan"."stg_dashboard__course_scripts"
),

unit_groups as (
    select * 
    from "dev"."dbt_jordan"."stg_dashboard__unit_groups"
),

course_names as (
    select *
    from "dev"."dbt_jordan"."dim_course_names"
),

script_names as (
    select * 
    from "dev"."dbt_jordan"."dim_script_names" 
),

combined as (
    select 
        -- courses
        ug.unit_group_id as course_id,
        ug.name as course_name,
        cn.course_name_short,
        cn.course_name_long,
        sc.course_name_true,

        -- scripts
        sl.script_id,
        sc.script_name,
        sn.versioned_script_name,
        sn.script_name_short,
        sn.script_name_long,
        
        -- stages
        st.stage_id,
        st.stage_name,
        st.stage_number,

        -- levels
        case when sl.script_id = '26' 
              and lsl.level_id = '14633' 
             then 1 else lsl.level_id 
        end as level_id,
        lev.name as level_name,
        sl.position as level_number,
        rank() over(partition by sl.script_id order by st.stage_number, sl.position) as level_script_order,
        sl.is_assessment,
        lev.updated_at
    from levels_script_levels as lsl 
    join script_levels as sl 
        on lsl.script_level_id = sl.script_level_id
    join levels as lev
        on lsl.level_id = lev.level_id  
    join scripts as sc 
        on sl.script_id = sc.script_id
    join stages as st 
        on sl.stage_id = st.stage_id 
    left join course_scripts as cs 
        on sc.script_id = cs.script_id
    left join unit_groups as ug 
        on cs.course_id = ug.unit_group_id
    left join course_names as cn 
        on ug.unit_group_id = cn.versioned_course_id   
    left join script_names as sn 
        on sc.script_id = sn.versioned_script_id
)

select * 
from combined