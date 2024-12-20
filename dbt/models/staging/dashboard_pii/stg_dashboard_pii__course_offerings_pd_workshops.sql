with 

base as ( 
    select 
        pd_workshop_id,
        course_offering_id,
        created_at,
        updated_at 
    from {{ ref('base_dashboard_pii__course_offerings_pd_workshops') }}
)

select * 
from base