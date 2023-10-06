with 
students as (
    select * 
    from {{ ref('dim_users')}}
    where user_type = 'student'
        and purged_at is null 
)

select * 
from students 