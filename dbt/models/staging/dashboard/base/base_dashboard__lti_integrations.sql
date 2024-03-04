with 
source as (
    select * 
    from {{ source('dashboard', 'lti_integrations') }}
)

select * 
from source
