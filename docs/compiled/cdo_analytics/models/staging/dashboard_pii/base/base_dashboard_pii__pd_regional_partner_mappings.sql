with 
source as (
      select * from "dashboard"."dashboard_production_pii"."pd_regional_partner_mappings"
),

renamed as (
    select
        id as pd_regional_partner_mapping_id,
        regional_partner_id,
        state,
        zip_code,
        created_at,
        updated_at,
        deleted_at
    from source
)

select * from renamed