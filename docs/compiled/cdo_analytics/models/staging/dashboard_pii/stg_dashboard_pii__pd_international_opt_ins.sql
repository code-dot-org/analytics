with 
 __dbt__cte__base_dashboard_pii__pd_international_opt_ins as (
with 
source as (
    select * 
    from "dashboard"."dashboard_production_pii"."pd_international_opt_ins"
),

renamed as (
    select 
        id as international_opt_in_id,
        user_id,
        form_data, -- would do well to unpack this in stg model
        created_at,
        updated_at
    from source 
)

select *
from renamed
), international_opt_ins as (
    select * 
    from __dbt__cte__base_dashboard_pii__pd_international_opt_ins
)

select * 
from international_opt_ins