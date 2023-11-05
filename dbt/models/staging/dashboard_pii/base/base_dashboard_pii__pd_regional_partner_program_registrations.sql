with 
source as (
    select * 
    from {{ source('dashboard_pii', 'pd_regional_partner_program_registrations') }}
),

renamed as (
    select
        id          as pd_regional_partner_program_registration_id,
        user_id,
        -- form_data,
        teachercon,
        created_at,
        updated_at
    from source
)

select * 
from renamed