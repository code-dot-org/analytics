with 
users as (
    select * 
    from {{ ref('base_dashboard_pii__users') }}
)

select * 
from users