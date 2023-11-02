with 
source as (
    select * 
    from {{ source('dashboard_pii', 'pd_regional_partner_contacts') }}
),

renamed as (
    select
        id as pd_regional_partner_contact_id,
        user_id,
        regional_partner_id,
        -- form_data,
        created_at,
        updated_at
    from source
)

select * 
from renamed