-- fka: int_user_levels
-- scope: capture user_level data in one model

with 
user_levels as (
    select * 
    from {{ ref('stg_dashboard__user_levels') }}
    where attempts > 0
),

users as (
    select *
    from {{ ref('dim_users') }}
),

course_structure as (
    select *
    from {{ ref('dim_course_structure') }}
),

combined as (
    select 
        -- user level id's 
        usl.user_id,
        usl.level_id,
        usl.script_id,
        
        -- user data
        usr.us_state,
        usr.country,
        usr.us_intl,
        usr.is_international,

        -- courses data 
        cs.course_name,
        cs.course_name_true,

        
        -- dates
        usl.created_at

    from user_levels    as usl 
    join users          as usr 
        on usl.user_id = usr.user_id
    
    join course_structure as cs 
        on usl.script_id    = cs.script_id
        and usl.level_id    = cs.level_id )

select *
from combined