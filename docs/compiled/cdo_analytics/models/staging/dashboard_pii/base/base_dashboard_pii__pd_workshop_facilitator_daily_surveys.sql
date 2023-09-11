with 
source as (
      select * from "dashboard"."dashboard_production_pii"."pd_workshop_facilitator_daily_surveys"
),

renamed as (
    select
        id as pd_workshop_facilitator_daily_survey_id,
        form_id,
        submission_id,
        user_id,
        pd_session_id,
        pd_workshop_id,
        facilitator_id,
        answers,
        day,
        created_at,
        updated_at
    from source
)

select * from renamed