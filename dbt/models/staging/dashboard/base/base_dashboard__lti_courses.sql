with
source as (
    select * 
    from {{ source('dashboard', 'lti_courses') }}
),

select * 
from source 