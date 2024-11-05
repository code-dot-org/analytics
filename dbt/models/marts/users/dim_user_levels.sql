-- fka: int_user_levels
-- scope: capture user_level data in one model


with 
user_levels as (
    select 
        user_id,
        level_id,
        script_id,
        {{ dbt_utils.generate_surrogate_key(
            ['level_id',
             'script_id']) }}   as level_script_id,
        created_at,
        created_at::date        as created_date,
        sum(time_spent)         as time_spent_minutes,
        sum(attempts)           as total_attempts,
        max(best_result)        as best_result
    from {{ ref('stg_dashboard__user_levels') }}    
    where created_at > {{ get_cutoff_date() }}
    {{ dbt_utils.group_by(6) }}
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
        -- user data
        usl.user_id,
        usr.user_type,
        usr.self_reported_state,
        usr.state,
        usr.country,
        usr.us_intl,
        usr.is_international,

        -- user level id's 
        usl.level_id,
        usl.script_id,
        cs.level_script_id,
        
        -- courses data 
        cs.course_name,
        cs.is_active_student_course,

        -- aggs 
        usl.time_spent_minutes,
        usl.total_attempts,
        usl.best_result,

        -- dates
        sy.school_year,
        usl.created_date 

    from user_levels    as usl 
    
    join users          as usr 
        on usl.user_id = usr.user_id
    
    join course_structure as cs 
         on usl.level_script_id = cs.level_script_id
    
    join school_years as sy 
        on usl.created_at
            between sy.started_at
                and sy.ended_at )

select *
from combined