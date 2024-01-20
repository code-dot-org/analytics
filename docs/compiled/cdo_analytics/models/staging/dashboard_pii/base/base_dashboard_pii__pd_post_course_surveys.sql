with 
source as (
    select * 
    from "dashboard"."dashboard_production_pii"."pd_post_course_surveys"
),

renamed as (
    select
        id,
        form_id,
        submission_id,
        -- answers,
        year,
        user_id,
        course,
        created_at,
        updated_at
    from source
)

select * 
from renamed