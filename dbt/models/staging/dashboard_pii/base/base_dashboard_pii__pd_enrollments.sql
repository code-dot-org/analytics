with 
source as (
    select * 
    from {{ source('dashboard_pii', 'pd_enrollments') }}
    where deleted_at is null
),

renamed as (
    select
        id                                          as pd_enrollment_id
        , pd_workshop_id
        , name
        -- first_name,
        -- last_name,
        -- email,
        , created_at                                as enrolled_at
        , updated_at
        , school                                    as user_entered_school
        , code
        , user_id                                   as teacher_id                   
        , survey_sent_at                            as survey_sent_dt
        , completed_survey_id           
        , school_info_id
        , properties
        , application_id
    from source
)

select * 
from renamed