with 
source as (
    select * 
    from "dashboard"."dashboard_production_pii"."pd_regional_partner_mini_contacts"
),

renamed as (
    select 
        id as pd_regional_partner_mini_contact_id,
        user_id,
        regional_partner_id,
        -- form_data,
        created_at,
        updated_at
    from source
)

select * 
from renamed