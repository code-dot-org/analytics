with 
potential_teachers as (
    select * 
    from {{ ref('base_dashboard_pii__potential_teachers') }})

select * 
from potential_teachers