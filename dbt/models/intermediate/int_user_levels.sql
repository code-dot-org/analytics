-- int_user_levels

with 
user_levels as (
    select * 
    from {{ ref('stg_dashboard__user_levels') }}
),

students as (
    select * 
    from {{ ref('dim_students') }}
    where student_id in (select user_id from user_levels)
),

final as (
    select 
        distinct user_levels.user_id,
        students.is_international,
        user_levels.level_id,
        user_levels.script_id
    from user_levels 
    join students 
        on user_levels.user_id = students.student_id 
)

select * 
from final