with pd_workshop_facilitators as (
    select * 
    from {{ ref('base_dashboard_pii__pd_workshops_facilitators') }}
)

select * 
from pd_workshop_facilitators