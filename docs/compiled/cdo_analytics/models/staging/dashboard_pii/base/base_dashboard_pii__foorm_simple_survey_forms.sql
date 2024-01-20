with 
source as (
    select * 
    from "dashboard"."dashboard_production_pii"."foorm_simple_survey_forms"
),

renamed as (
    select
        id as foorm_simple_survey_form_id,
        path,
        kind,
        form_name,
        form_version,
        -- properties, 
        created_at,
        updated_at
    from source
)

select * 
from renamed