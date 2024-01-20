with
 __dbt__cte__base_dashboard_pii__pd_sessions as (
with 
source as (
    select * 
    from "dashboard"."dashboard_production_pii"."pd_sessions"
    where not deleted_at
),

renamed as (
    select
        id                  as pd_session_id,
        pd_workshop_id,
        "start"             as started_at,
        "end"               as ended_at,
        created_at,
        updated_at,
        code
    from source
)

select * 
from renamed
), pd_sessions as (
    select * 
    from __dbt__cte__base_dashboard_pii__pd_sessions
)

select * 
from pd_sessions