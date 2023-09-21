with
pd_sessions as (
    select * 
    from {{ ref('base_dashboard_pii__pd_sessions') }}
)

select * 
from pd_sessions