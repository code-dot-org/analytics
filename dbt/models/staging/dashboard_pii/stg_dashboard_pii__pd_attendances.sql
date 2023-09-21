with
pd_attendances as (
    select * 
    from {{ ref('base_dashboard_pii__pd_attendances') }}
)

select * 
from pd_attendances