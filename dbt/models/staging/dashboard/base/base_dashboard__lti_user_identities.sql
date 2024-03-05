with
source as (
    select * 
    from {{ source('dashboard', 'lti_user_identities') }}
),

renamed as (
    select
        id as lti_user_identities_id,
        subject,
        lti_integration_id,
        user_id,
        created_at,
        updated_at,
        deleted_at
    from source)

select * 
from renamed