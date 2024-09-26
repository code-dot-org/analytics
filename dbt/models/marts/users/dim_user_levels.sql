-- fka: int_user_levels
-- scope: capture user_level data in one model


with 
user_levels as (
    select *
    from {{ ref('stg_dashboard__user_levels') }}    
),

users as (
    select *
    from {{ ref('dim_users') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
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
        usr.user_type,
        usr.self_reported_state,
        usr.country,
        usr.us_intl,
        usr.is_international,

        -- courses data 
        cs.course_name,

        -- dates
        sy.school_year,
        usl.created_at::date                    as created_date,
        min(usl.created_at)                     as first_activity_at,
        max(usl.created_at)                     as last_activity_at,
        -- aggs 
        sum(usl.time_spent)                 as time_spent_minutes,
        sum(usl.attempts)                   as total_attempts,
        max(usl.best_result)                as best_result

    from user_levels    as usl 
    
    join users          as usr 
        on usl.user_id = usr.user_id
    
    join course_structure as cs 
         on usl.script_id = cs.script_id
        and usl.level_id  = cs.level_id 
    
    join school_years as sy 
        on usl.created_at
            between sy.started_at
                and sy.ended_at 
                
    {{ dbt_utils.group_by(11) }} )

select *
from combined