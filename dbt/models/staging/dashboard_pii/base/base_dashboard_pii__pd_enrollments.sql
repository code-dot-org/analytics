with 
source as (
      select * from {{ source('dashboard_pii', 'pd_enrollments') }}
),

renamed as (
    select
        id as pd_enrollment_id,
        pd_workshop_id,
        name,
        first_name,
        last_name,
        email,
        created_at,
        updated_at,
        school,
        code,
        user_id,
        survey_sent_at,
        completed_survey_id,
        school_info_id,
        deleted_at,
        properties,
        application_id
    from source
)

select * from renamed