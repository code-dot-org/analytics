with  __dbt__cte__base_dashboard_pii__pd_workshops_facilitators as (
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
), pd_workshop_facilitators as (
    select * 
    from __dbt__cte__base_dashboard_pii__pd_workshops_facilitators
)

select * 
from pd_workshop_facilitators