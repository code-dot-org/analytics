with 
source as (
      select * from "dashboard"."dashboard_production_pii"."pd_teachercon1819_registrations"
),

renamed as (
    select
        id as pd_teachercon1819_registration_id,
        pd_application_id,
        form_data,
        created_at,
        updated_at,
        regional_partner_id,
        user_id
    from source
)

select * from renamed