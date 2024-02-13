-- int_user_levels

with 
user_levels as (
    select * 
    from {{ ref('stg_dashboard__user_levels') }}
),

students as (
    select * 
    from {{ ref('dim_students') }}
),

final as (
    select 
        distinct user_levels.user_id,
        students.is_international,
        user_levels.level_id,
        user_levels.script_id
    from user_levels 
    join students 
        on user_levels.user_id = students.user_id 
)

select * 
from final