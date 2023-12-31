with 
source as (
    select * 
    from {{ source('dashboard_pii', 'pd_workshop_surveys') }}
),

renamed as (
    select
        id as pd_workshop_survey_id,
        pd_enrollment_id,
        -- form_data,
        created_at,
        updated_at,
        type
    from source
)

select * 
from renamed