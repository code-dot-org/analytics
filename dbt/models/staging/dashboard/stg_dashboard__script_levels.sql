with 
script_levels as (
    select * 
    from {{ ref('base_dashboard__script_levels')}}
)

select * 
from script_levels