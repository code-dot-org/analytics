with 

pd_applications_status_logs as (
    select * 
    from {{ ref('base_dashboard_pii__pd_applications_status_logs') }}
)

select * 
from pd_applications_status_logs