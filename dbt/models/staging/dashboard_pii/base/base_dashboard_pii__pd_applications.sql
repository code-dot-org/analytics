with 
source as (
    select * 
    from {{ source('dashboard_pii', 'pd_applications') }}
    where deleted_at is null
),

renamed as (
    select
        id                                  as pd_application_id
        , user_id                           as teacher_id
        , application_year
        , application_type
        , regional_partner_id
        , status                            as current_status
        , locked_at
        -- notes,
        , form_data
        , created_at
        , updated_at
        , course                            as course_name
        , response_scores
        , application_guid
        , accepted_at
        , properties
        , status_timestamp_change_log
        , applied_at
    from source
)

select * 
from renamed