-- fka: int_user_levels
-- scope: capture user_level data in one model
{{
    config(
        materialized='incremental',
        unique_key=['user_id','created_at']
    )
}}

with 
user_levels as (
    select *, 
        date_trunc('day', created_at)    as activity_date,
        extract('month' from created_at) as activity_month
   
    from {{ ref('stg_dashboard__user_levels') }}
    
    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    -- (uses >= to include records arriving later on the same day as the last run of this model)
    
    where created_at >= (select max(created_at,updated_dt) from {{ this }})
    -- where attempts > 0

    {{ dbt_utils.group_by(5) }}
    
    {% endif %}
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

        -- school year 
        sy.school_year,
        
        -- dates
        usl.activity_date,
        usl.activity_month, 
        
        -- aggs 
        sum(usl.time_spent) as time_spent_minutes,
        sum(usl.attempts) as total_attempts,
        max(best_result) as best_result

    from user_levels    as usl 
    
    join users          as usr 
        on usl.user_id = usr.user_id
    
    join course_structure as cs 
         on usl.script_id = cs.script_id
        and usl.level_id  = cs.level_id 
    
    join school_years as sy 
        on usl.activity_date
            between sy.started_at
                and sy.ended_at 
                
    {{ dbt_utils.group_by(12) }} )

select *
from combined