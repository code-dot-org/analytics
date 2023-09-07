with 
source as (
      select * from {{ source('dashboard_pii', 'pd_workshop_survey_foorm_submissions') }}
),

renamed as (
    select
        id as pd_workshop_survey_foorm_submission_id,
        foorm_submission_id,
        user_id,
        pd_session_id,
        pd_workshop_id,
        day,
        created_at,
        updated_at,
        facilitator_id,
        workshop_agenda
    from source
)

select * from renamed