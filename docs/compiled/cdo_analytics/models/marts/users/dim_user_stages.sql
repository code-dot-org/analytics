with
user_levels as (
    select *
    from "dev"."dbt_jordan"."stg_dashboard__user_levels"
),

levels_script_levels as (
    select * 
    from "dev"."dbt_jordan"."stg_dashboard__levels_script_levels"
),

script_levels as (
    select * 
    from "dev"."dbt_jordan"."stg_dashboard__script_levels"
),

stages as (
    select * 
    from "dev"."dbt_jordan"."stg_dashboard__stages"
),

levels as (
    select * 
    from "dev"."dbt_jordan"."stg_dashboard__levels"
    where type != 'StandaloneVideo'
    -- excludes problematic levels shared across the multiple stages in the same script, which we can't differentiate
),

course_scripts as (
    select * 
    from "dev"."dbt_jordan"."stg_dashboard__course_scripts"
),

final as (
    select 
        ul.user_id, 
        cs.course_id, 
        sl.script_id, 
        st.stage_id, 
        st.absolute_position      as stage_number, 
        count(*)                  as levels_attempted,
        min(ul.created_at)::date  as stage_started_at,
        max(ul.updated_at)        as updated_at
    from user_levels ul
    join levels_script_levels lsl on lsl.level_id = ul.level_id
    join script_levels sl on sl.script_level_id = lsl.script_level_id and sl.script_id = ul.script_id
    join stages st on st.stage_id = sl.stage_id
    join levels le on le.level_id = lsl.level_id
    left join course_scripts cs on cs.script_id = ul.script_id
    group by 1,2,3,4,5
)

select * 
from final