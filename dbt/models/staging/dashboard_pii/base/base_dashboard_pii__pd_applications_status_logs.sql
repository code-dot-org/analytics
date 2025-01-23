with 
source as (
    select * 
    from {{ source('dashboard_pii', 'pd_applications_status_logs') }}
)

select 
    id                                      as application_status_id
    , pd_application_id
    , status                                as application_status
    , timestamp                             as changed_status_dt
    , position                              as change_order
from source