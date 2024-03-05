with 
lti_courses as (
    select * 
    from {{ ref('base_dashboard__lti_courses') }}
),

renamed as (
    select *
    from lti_courses
)

select * 
from renamed