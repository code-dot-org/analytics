with 
user_levels as (
    select * 
    from {{ ref('base_dashboard__user_levels') }}
)

select * 
from user_levels