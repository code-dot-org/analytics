with 
source as (
    select * 
    from {{ source('dashboard_pii', 'pd_regional_partner_mappings') }}
    where deleted_at is null
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