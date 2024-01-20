with 
source as (
    select * 
    from "dashboard"."dashboard_production_pii"."pd_teachercon_surveys"
),

renamed as (
    select
        id as pd_teachercon_survey_id,
        pd_enrollment_id,
        -- form_data,
        created_at,
        updated_at
    from source
)

select * 
from renamed