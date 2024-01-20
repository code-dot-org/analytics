with 
source as (
    select * 
    from "dashboard"."dashboard_production"."courses"
),

renamed as (
    select
        id as course_id,
        name,
        properties,
        created_at,
        updated_at
    from source
)

select * 
from renamed