with 
user_proficiencies as (
    select * 
    from {{ ref('base_dashboard__user_proficiencies')}}
)

select * 
from user_proficiencies