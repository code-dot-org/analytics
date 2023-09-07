with
pd_applications as (
    select * 
    from {{ ref('base_dashboard_pii__pd_applications') }}
)

select * 
from pd_applications