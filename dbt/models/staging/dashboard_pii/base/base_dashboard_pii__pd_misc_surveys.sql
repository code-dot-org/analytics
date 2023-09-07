with 
source as (
      select * from {{ source('dashboard_pii', 'pd_misc_surveys') }}
),

renamed as (
    select
        id as pd_misc_survey_id,
        form_id,
        submission_id,
        answers,
        user_id,
        created_at,
        updated_at
    from source
)

select * from renamed