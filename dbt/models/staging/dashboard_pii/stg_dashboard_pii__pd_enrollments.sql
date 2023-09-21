with
pd_enrollments as (
    select * 
    from {{ ref('base_dashboard_pii__pd_enrollments') }}
)

select * 
from pd_enrollments