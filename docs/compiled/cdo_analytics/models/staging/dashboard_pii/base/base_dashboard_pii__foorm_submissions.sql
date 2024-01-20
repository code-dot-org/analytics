with 
source as (
    select * 
    from "dashboard"."dashboard_production_pii"."foorm_submissions"
),

renamed as (
    select
        id as foorm_submission_id,
        form_name,
        form_version,
        -- answers,
        created_at,
        updated_at
    from source
)

select * 
from renamed