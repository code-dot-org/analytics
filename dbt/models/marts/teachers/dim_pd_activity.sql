-- model: dim_pd_activity
-- scope: user_levels where 
--        participant_audience = teacher
-- auth: js; 2024-07-14
-- note: eventually, replace user_levels + course_structure
--       with dim_course_utilization

with course_structure as (
    select *
    from {{ ref('dim_course_structure') }}
    where participant_audience = 'teacher'
    and instruction_type = 'self-paced'
),

user_levels as (
    select * 
    from {{ref('dim_user_levels')}}
    where user_type = 'teacher'
        and is_international = 0 
),

combined as (
    select 

        -- users 
        ul.user_id,
        ul.level_id,
        ul.script_id,
        cs.

        ul.self_reported_state,
        ul.country,
        ul.us_intl,

        -- dates     
        ul.created_at,
        ul.updated_at
    from user_levels        as ul 
    join course_structure   as cs 
        on  ul.script_id = cs.script_id
        and ul.level_id = cs.level_id 
)

