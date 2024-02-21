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

combined as (
    select 
        user_levels.user_id,
        users.is_international,
        user_levels.level_id,
        user_levels.script_id,
        user_levels.created_at,
        user_levels.updated_at
    from user_levels
    join users 
        on user_levels.user_id = users.user_id
)

select * 
from final