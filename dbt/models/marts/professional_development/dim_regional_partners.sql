with 

regional_partners as (
    select * 
    from {{ ref('stg_dashboard_pii__regional_partners') }}
),

rp_mappings as (
    select * 
    from {{ ref('stg_dashboard_pii__pd_regional_partner_mappings') }}
)

select distinct
    regional_partners.regional_partner_id
    , regional_partners.regional_partner_name
    --, regional_partners.is_urban  # questioning the accuracy of this field
    , coalesce(rp_mappings.state, nullif(regional_partners.state, '')) as state
    --, rp_mappings.zip_code
    --, is_active - # questioning the accuracy of this field 
from rp_mappings
left join regional_partners 
    on rp_mappings.regional_partner_id = regional_partners.regional_partner_id
