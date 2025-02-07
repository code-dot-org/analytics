with international_partners_raw as (
    select *
        {{country_normalization(diplay_name)}} as country,
        display_name as country
        lower(partner) as partner,
        lower(workshop_organizers) as workshop_organizers,
        lower(partner_type) as partner_type,
        partner_id
    from {{ ref ('base_public__international_partners_raw') }}
)

select * 
from international_partners_raw