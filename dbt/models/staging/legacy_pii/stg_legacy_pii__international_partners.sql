with 
international_partners as (
    select * 
    from {{ ref('base_legacy_pii__international_partners')}}
),

renamed as (
    select         
        -- partner data
        partner_id,
        partner                                                                             as partner_name,
        case when partner_type = '' then 'public' else partner_type end                     as partner_type,

        -- country data 
        country_cd                                                                          as country_code,
        lower(display_name)                                                                 as country_name,
        lower(alt_name)                                                                     as alt_country_name,
        region,

        --contact info
        case when workshop_organizers = '' 
            then 'no partner' 
            else lower(workshop_organizers) end                                             as workshop_organizers,
        contact_name,
        contact_email -- @allison is this approved as well? (js) 
        -- exceptions -- unclear if needed, removing for now (js)
    from international_partners
)

select * 
from renamed 