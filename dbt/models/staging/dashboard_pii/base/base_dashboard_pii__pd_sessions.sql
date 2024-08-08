with 
source as (
    select * 
    from {{ source('dashboard_pii', 'pd_sessions') }}
    where deleted_at is null
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