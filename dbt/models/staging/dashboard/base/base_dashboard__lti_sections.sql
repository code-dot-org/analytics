with
source as (
    select *
    from {{ ref('base_dashboard__lti_sections') }}
)

select * 
from source 