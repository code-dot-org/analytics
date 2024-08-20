with countries as (
    select * 
    from {{ ref ('base_public__countries') }}
)

select 
    country_cd
    , partner_id                                as intl_partner_id
    , lower(display_name)                       as display_name
    , lower(alt_name)                           as alt_name
    , lower(region)                             as region
from countries