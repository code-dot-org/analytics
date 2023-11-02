with 
source as (
    select * 
    from {{ source('pegasus_pii', 'form_geos') }}
),

renamed as (
    select
        id as form_geo_id,
        form_id,
        created_at,
        updated_at,
        {# ip_address, #}
        city,
        state,
        country,
        postal_code
        {# latitude,
        longitude,
        indexed_at #}
    from source
)

select * 
from renamed
  