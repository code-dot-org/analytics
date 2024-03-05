with
lti_user_identities as (
    select * 
    from {{ ref('base_dashboard__lti_user_identities') }}
),

final as (
    select * 
    from lti_user_identities)

select * 
from final 