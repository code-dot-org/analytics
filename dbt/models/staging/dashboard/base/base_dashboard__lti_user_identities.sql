with
source as (
    select * 
    from {{ source('dashboard', 'lti_user_identities') }}
),

select * 
from source 