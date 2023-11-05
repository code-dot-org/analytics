with 
source as (
    select * 
    from {{ source('dashboard_pii', 'foorm_simple_survey_submissions') }}
),

renamed as (
    select
        id as foorm_simple_survey_submission_id,
        foorm_submission_id,
        user_id,
        simple_survey_form_id,
        misc_form_path,
        created_at,
        updated_at
    from source
)

select * 
from renamed