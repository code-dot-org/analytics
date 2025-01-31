with 
source as (
    select * 
    from {{ source('dashboard_pii', 'pd_attendances') }}
    where deleted_at is null
),

renamed as (
    select
        id as pd_attendance_id,
        pd_session_id,
        teacher_id,
        created_at,
        updated_at,
        pd_enrollment_id,
        marked_by_user_id
    from source
)

select * 
from renamed