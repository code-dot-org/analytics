with 
lti_integrations as (
    select * 
    from {{ ref('base_dashboard__lti_integrations') }}
),

final as (
    select *
    from lti_integrations)

select *
from final 