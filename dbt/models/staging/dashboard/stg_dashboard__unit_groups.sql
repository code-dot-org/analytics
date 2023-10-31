with 
unit_groups as (
    select * 
    from {{ ref('base_dashboard__unit_groups')}}
)

select * 
from unit_groups