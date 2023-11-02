with regional_partners as (
    select * 
    from {{ ref('base_dashboard_pii__regional_partners') }}
)

select * 
from regional_partners