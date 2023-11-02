with 
source as (
    select * 
    from {{ source('dashboard_pii', 'pd_workshop_daily_surveys') }}
),

renamed as (
    select
        id as pd_workshop_daily_survey_id,
        form_id,
        submission_id,
        user_id,
        pd_session_id,
        pd_workshop_id,
        -- answers,
        day,
        created_at,
        updated_at
    from source
)

select * 
from renamed