with 
form_geos as (
    select 
        form_geo_id,
        form_id,
        created_at,
        updated_at,
        lower(city) as city,
        lower(state) as state,
        {{country_normalization('country')}} as country,
        postal_code
    from {{ ref('base_pegasus_pii__form_geos') }}
)

select * 
from form_geos