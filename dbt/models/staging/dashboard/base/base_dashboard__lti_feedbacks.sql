with 
source as (
    select * 
    from {{ source('dashboard', 'lti_feedbacks') }}
)

select * 
from source