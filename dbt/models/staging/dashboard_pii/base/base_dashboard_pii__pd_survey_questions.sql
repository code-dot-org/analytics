with 
source as (
    select * 
    from {{ source('dashboard_pii', 'pd_survey_questions') }}
),

renamed as (
    select
        id as pd_survey_question_id,
        form_id,
        questions,
        created_at,
        updated_at,
        last_submission_id
    from source
)

select * 
from renamed