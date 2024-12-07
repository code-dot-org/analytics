with 

regional_partners as (
    select * 
    from {{ ref('stg_dashboard_pii__regional_partners') }}
)

select 
    regional_partner_id
    , regional_partner_name
    , is_urban
    , address
    , apt_num
    , city
    , state
    , zip_code
    , is_active
from regional_partners
