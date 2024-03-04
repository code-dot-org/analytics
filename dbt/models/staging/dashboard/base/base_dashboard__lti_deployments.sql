with
source as (
    select * 
    from {{ source('dashboard', 'lti_deployments') }}
)

select * 
from source