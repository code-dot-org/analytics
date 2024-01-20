with  __dbt__cte__base_dashboard_pii__pd_regional_partner_mappings as (
with 
source as (
    select * 
    from "dashboard"."dashboard_production_pii"."pd_regional_partner_mappings"
    where not deleted_at
),

renamed as (
    select
        id as pd_regional_partner_mapping_id,
        regional_partner_id,
        state,
        zip_code,
        created_at,
        updated_at
    from source
)

select * 
from renamed
), pd_regional_partner_mappings as (
    select * 
    from __dbt__cte__base_dashboard_pii__pd_regional_partner_mappings
)

select * 
from pd_regional_partner_mappings