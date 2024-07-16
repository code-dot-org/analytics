-- fka: int_user_levels
-- scope: capture user_level data in one model
-- note: this model can be expanded much more

with 
user_levels as (
    select * 
    from {{ ref('stg_dashboard__user_levels') }}
),

users as (
    select *
    from {{ ref('dim_users') }}
),

combined as (
    select 
        users.user_id,
        users.user_type,
        
        -- levels data 
        user_levels.level_id,
        user_levels.script_id,
        
        -- user geo data
        users.self_reported_state,
        users.country,
        users.us_intl,
        users.is_international,
    
        -- dates     
        user_levels.created_at,
        user_levels.updated_at

    from user_levels
    join users 
        on user_levels.user_id = users.user_id )

select * 
from combined