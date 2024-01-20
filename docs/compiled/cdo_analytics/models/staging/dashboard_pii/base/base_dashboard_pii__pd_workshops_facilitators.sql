with 
source as (
    select * 
    from "dashboard"."dashboard_production_pii"."pd_workshops_facilitators"
),

renamed as (
    select
        pd_workshop_id,
        user_id
    from source
)

select * 
from renamed