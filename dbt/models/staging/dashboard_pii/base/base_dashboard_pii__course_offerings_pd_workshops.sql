with 
source as (
    select 
        pd_workshop_id,
        course_offering_id,
        created_at,
        updated_at 
    from {{ source('dashboard_pii', 'course_offerings_pd_workshops') }}
)

select * 
from source