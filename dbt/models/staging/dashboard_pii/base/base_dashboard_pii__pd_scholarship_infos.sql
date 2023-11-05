with 
source as (
    select * 
    from {{ source('dashboard_pii', 'pd_scholarship_infos') }}
),

renamed as (
    select
        id as pd_scholarship_info_id,
        user_id,
        application_year,
        scholarship_status,
        pd_application_id,
        pd_enrollment_id,
        created_at,
        updated_at,
        course
    from source
)

select * 
from renamed