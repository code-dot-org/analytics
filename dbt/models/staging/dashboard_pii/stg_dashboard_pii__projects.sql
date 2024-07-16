with projects as (
    select *
    from {{ ref('base_dashboard_pii__projects') }} 
    where state = 'active' )

select *
from projects