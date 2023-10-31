with 
level_script_levels as (
    select * 
    from {{ ref('base_dashboard__levels_script_levels')}}
)

select * 
from level_script_levels