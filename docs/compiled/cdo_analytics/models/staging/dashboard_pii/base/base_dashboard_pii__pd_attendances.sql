with 
source as (
      select * from "dashboard"."dashboard_production_pii"."pd_attendances"
),

renamed as (
    select
        id as pd_attendance_id,
        pd_session_id,
        teacher_id,
        created_at,
        updated_at,
        deleted_at,
        pd_enrollment_id,
        marked_by_user_id
    from source
)

select * from renamed