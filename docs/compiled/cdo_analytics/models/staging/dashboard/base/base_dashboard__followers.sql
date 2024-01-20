with 
source as (
    select * 
    from "dashboard"."dashboard_production"."followers"
    where deleted_at is null 
),

renamed as (
    select
        id as follower_id,
        student_user_id as student_id,
        section_id,
        created_at,
        updated_at
    from source
)

select * 
from renamed