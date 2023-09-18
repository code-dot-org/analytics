with 
source as (
      select * from "dashboard"."dashboard_production_pii"."pd_teacher_applications"
),

renamed as (
    select
        id as pd_teacher_application_id,
        created_at,
        updated_at,
        user_id,
        primary_email,
        secondary_email,
        application,
        regional_partner_override,
        program_registration_id
    from source
)

select * from renamed