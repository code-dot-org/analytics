with 
lti_sections as (
    select * 
    from {{ ref('base_dashboard__lti_sections') }}
),

final as (
    select * 
    from lti_sections)

select * 
from final 