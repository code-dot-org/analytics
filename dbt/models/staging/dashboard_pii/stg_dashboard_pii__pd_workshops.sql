with
pd_workshops as (
    select * 
    from {{ ref('base_dashboard_pii__pd_workshops') }}
)

select * 
from pd_workshops