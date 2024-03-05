with
source as (
    select * 
    from {{ source('dashboard', 'lti_deployments') }}
),

renamed as (
    select 
        id as lti_deployment_id,
        deployment_id,
        lti_integration_id,
        created_at,
        updated_at
    from source)

select * 
from