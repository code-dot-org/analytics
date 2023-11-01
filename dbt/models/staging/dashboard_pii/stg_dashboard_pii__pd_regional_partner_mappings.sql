with pd_regional_partner_mappings as (
    select * 
    from {{ ref('base_dashboard_pii__pd_regional_partner_mappings') }}
)

select * 
from pd_regional_partner_mappings