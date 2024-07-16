-- fka: int_user_levels
-- scope: capture user_level data in one model

with 
user_levels as (
    select *, 
        cast(created_at as date) as created_dt,
        cast(updated_at as date) as updated_dt 
    from {{ ref('stg_dashboard__user_levels') }}
    where attempts > 0
),

users as (
    select *
    from {{ ref('dim_users') }}
),

course_structure as (
    select 
        course_name,
        script_id,
        level_id 
    from {{ ref('dim_course_structure') }}
),

combined as (
    select 
        -- user level id's 
        usl.user_id,
        usl.level_id,
        usl.script_id,
        -- user data
        usr.self_reported_state,
        usr.country,
        usr.us_intl,
        usr.is_international,

        -- courses data 
        cs.course_name,

        -- agg's
        usl.time_spent,
        usl.attempts,
        usl.best_result,
        
        -- dates
        coalesce(
            usl.updated_dt,
            usl.created_dt) as last_activity_at 

    from user_levels    as usl 
    join users          as usr 
        on usl.user_id = usr.user_id
    
    join course_structure as cs 
        on usl.script_id    = cs.script_id
        and usl.level_id    = cs.level_id )

select *
from combined